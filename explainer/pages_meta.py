"""The metadata store: the hub, the config data model, settings, functions, the loader and the runtime tables."""
from lib import *  # noqa: F401,F403

SQL = "P/src/dataplatform/adapters/sql/postgres_meta.sql"
OBS = "P/src/dataplatform/adapters/sql/postgres_obs.sql"
PORT = "P/src/dataplatform/meta/port.py"
LOADER = "P/src/dataplatform/meta/loader.py"
REGISTRY = "P/src/dataplatform/meta/registry.py"
REGJSON = "P/src/dataplatform/meta/settings_registry.json"
PG = "P/src/dataplatform/adapters/postgres_meta.py"
MEM = "P/src/dataplatform/adapters/memory_meta.py"
LAKE = "P/src/dataplatform/adapters/lakebase.py"
KEYS = "P/src/dataplatform/keys.py"
LEASES = "P/src/dataplatform/leases.py"
T_STORE = "P/tests/dataplatform/test_meta_store.py"
T_LOADER = "P/tests/dataplatform/test_meta_loader.py"
T_LAKE = "P/tests/dataplatform/test_lakebase.py"
TYRE = "C/slices/02-tyre/manifest/"
END = r"^END \$\$;"


def cols(rows):
    """A table of a database table's columns: name, type, what it is for."""
    return table(["Column", "Type and rule", "What it is for"], [[f"<code>{a}</code>", b, c] for a, b, c in rows])


# ------------------------------------------------------------------------------------------------------------- hub
def hub_shape():
    d = box(10, 30, 170, 80, "Manifests in git", ["YAML files,", "reviewed"], "bxm")
    d += box(230, 30, 160, 80, "Loader", ["checks everything,", "then writes"], "bx", href="meta-loader.html")
    d += box(10, 230, 170, 80, "Pipelines", ["ingestion, silver,", "gold, housekeeping"], "bxm")
    d += box(440, 110, 190, 120, "Functions", ["the only way in", "or out"], "solid", href="meta-functions.html")
    d += box(710, 20, 260, 80, "config schema", ["what to load and how"], "bxs", href="meta-model.html")
    d += box(710, 135, 260, 70, "Settings merge", ["default, domain, dataset"], "bx", href="meta-settings.html")
    d += box(710, 240, 260, 80, "runtime schema", ["what happened"], "bxs", href="meta-runtime.html")
    d += arrow(180, 70, 228, 70) + arrow(390, 70, 438, 140)
    d += arrow(180, 270, 438, 205)
    d += arrow(630, 140, 708, 65) + arrow(630, 170, 708, 170) + arrow(630, 200, 708, 275)
    return d


@page("meta", "The metadata store: configuration and run state in one database", parent="index",
      short="Metadata store",
      blurb="Two schemas in one PostgreSQL database. Pipelines never touch a table. They call functions.")
def meta():
    out = ("<p class='lead'>A metadata-driven platform is one where the code is the same for every dataset and the "
           "differences between datasets are data. The metadata store is the database that holds that data: which "
           "datasets exist, how each one is loaded, and what happened in every run.</p>"
           "<p>Without it, each difference between datasets ends up as an <code>if</code> in a job, and the record "
           "of what ran ends up in log files that nobody can query. With it, adding a dataset is adding rows, and "
           "the question of what the platform did last night is one query.</p>")
    out += fig(980, 340, hub_shape(), "The parts of the metadata store",
               "The highlighted box is the gate. The loader and every pipeline pass through it. Click a box to zoom in.")
    out += "<h2>The pages under this one</h2>"
    out += table(["Page", "What it answers"], [
        [link("meta-tables"), "All 24 tables at a glance: what each one is for and an example row. Start here."],
        [link("meta-model"), "Which configuration tables exist, every column in them, and how they point at each other."],
        [link("meta-settings"), "Which settings are allowed, where a value can be set, and how the final value is chosen."],
        [link("meta-functions"), "Every function in the store, what it does, what it refuses and who calls it."],
        [link("meta-loader"), "How YAML files in git become rows, and why nothing is written until everything is checked."],
        [link("meta-runtime"), "The tables that record runs, files, errors, watermarks, bookmarks and leases, and the rules that keep them honest."],
    ])
    out += "<h2>The design decisions</h2>"
    out += table(["Decision", "What it means", "Why"], [
        ["Configuration lives in a database, not in code",
         "A dataset's settings, columns and keys are rows. The pipelines read them at run time through one merged "
         "view. They never open a YAML file.",
         "A run can record exactly which configuration it used. A setting can be queried, compared and audited. "
         "The same pipeline code serves every dataset."],
        ["Git is still the source",
         "People edit YAML manifests in git. A loader validates them and writes the rows. Nothing else writes "
         "configuration.",
         "A change to configuration gets a review and a history, like code."],
        ["Two schemas",
         "<code>config</code> holds what to load and how. <code>runtime</code> holds what happened.",
         "The two change at different speeds and are written by different actors. The loader writes "
         "<code>config</code>. Running pipelines write <code>runtime</code>."],
        ["Functions are the only way in",
         "Every table is revoked from public access. A service role is granted the right to execute the functions "
         "and nothing else.",
         "The rules live in one place. A pipeline with a bug cannot move a watermark for a failed run, because "
         "the only function that moves a watermark checks the status first."],
        ["One adapter for any PostgreSQL",
         "The same SQL file and the same Python adapter serve a local server, a managed server and the engine's "
         "managed PostgreSQL. Only the connection string changes.",
         "There is no second code path to test per product."],
    ])
    out += code(SQL, r"^-- The functions below are the only way in", 4,
                "The header of the SQL file states the rule and the four refusal codes.")
    out += "<h2>The four refusal codes</h2>"
    out += ("<p>When a function refuses a call it raises an error with one of four codes of our own. The Python "
            "adapter turns each code into one exception class, so the calling code can tell a typing mistake from "
            "a run that is out of order.</p>")
    out += table(["Code", "Meaning", "Python exception", "A real example from the SQL"], [
        ["<code>DPM01</code>", "Unknown setting", "<code>UnknownSetting</code>", "A manifest sets a key that is not in the dictionary of allowed settings."],
        ["<code>DPM02</code>", "Not found", "<code>NotFound</code>", "A dataset that is not loaded, a domain that is not loaded, a run that was never started."],
        ["<code>DPM03</code>", "Invalid", "<code>InvalidArgument</code>", "A run type that is not normal, reload or backfill. A watermark reset whose reason is shorter than 10 characters."],
        ["<code>DPM04</code>", "Out of order", "<code>RunStateError</code>", "Starting the same run twice. Finishing a run that still has a dataset running. Logging a step for a dataset that was not started in that run."],
    ])
    out += code(PG, r"^ERRORS: dict", 2, "The adapter's map from refusal code to exception.")
    out += code(PG, r"    def _call\(self, sql", caption="Every call goes through this method. A refusal code becomes its exception. Any other database error is raised as it is.")
    out += "<h2>A worked example: one call, one refusal</h2>"
    out += ("<p>The tyre company's ingestion run starts the dataset run for the sales order lines and later closes "
            "it. Each line below is one function call the pipeline makes and what the store answers.</p>")
    out += table(["Call", "Answer"], [
        ["<code>runtime.start_run('ingest-2026-11-04', 'tyre', 'dev', 'schedule', 'normal')</code>", "A new row in <code>runtime.run</code> with status <code>running</code>."],
        ["The same call again", "Refused with <code>DPM04</code>: the run was already started."],
        ["<code>runtime.start_dataset_run(run, 'erp', 'sales_order_lines', ...)</code> with the dataset name misspelt", "Refused with <code>DPM02</code>: the dataset is not loaded."],
        ["<code>runtime.finish_dataset_run(run, 'erp', 'sales_order_line', 'done', ...)</code>", "Refused with <code>DPM03</code>: the status is not succeeded, failed or skipped."],
    ])
    out += "<h2>How the gate is granted</h2>"
    out += code(PG, r"^def install\(", caption="Installing the store. With a service role, the role gets the functions and no table.")
    out += ("<p>The SQL file itself revokes every table from public access. The three grant statements for a "
            "service role are in this Python function, not in the SQL file. The header comment of the SQL file "
            "says to look at the end of that file for them, which is out of date.</p>")
    out += "<h2>The settings the store itself reads</h2>"
    out += table(["Name", "What it means", "Default"], [
        ["<code>DP_META_DSN</code>", "Environment variable with the connection string of a plain PostgreSQL.", "<code>dbname=dp_meta</code>"],
        ["<code>DP_META_INSTANCE</code>", "Environment variable naming a managed PostgreSQL instance of the engine. When set, the job connects as its own identity with a short-lived token.", "Not set"],
        ["<code>DP_META_DATABASE</code>", "The database name on that instance.", "<code>dp_meta</code>"],
    ])
    out += code(LAKE, r"^def meta_store_from_env\(", caption="The binding's choice comes from the environment. Either way the same adapter class is returned.")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_STORE)} holds one table of 112 cases. Every case runs against two adapters: the store in "
            "memory and the store on PostgreSQL. The PostgreSQL half is skipped when no local server answers. "
            "Two further tests prove the gate. The first reads the adapter's source and checks that every "
            "statement it sends is a call to a function. The second connects as a service role and checks that "
            "calling a function works and reading or writing a table is denied.</p>")
    out += code(T_STORE, r"^def test_adapter_only_calls_procedures", caption="The static half of the proof: no statement in the adapter names a table.")
    out += code(T_STORE, r'        \("call a reader procedure"', 6, "The dynamic half: the five things a service role tries, and whether each is allowed.")
    out += not_built([
        "The store has no function that returns the domain row. The domain's time zone, its environments and the "
        "whole platform manifest are stored, but a pipeline cannot read them back through the gate. The bindings "
        "pass the time zone in from the manifest files instead.",
        "There is one real adapter, for PostgreSQL, and one in memory for tests. No adapter exists for another "
        "database family.",
        "Schema changes are made by running the SQL file again. Two columns were added in place with "
        f"<code>ALTER TABLE</code> lines in the file. There is no versioned migration tool. See {link('meta-runtime')}.",
        "I could not verify a run against the engine's managed PostgreSQL from the code alone. "
        f"{src(T_LAKE)} tests the connection logic with fakes.",
    ])
    return out


# ---------------------------------------------------------------------------------------------------------- tables
CONFIG_TABLES = [
    ("config.setting_def", "The registry of every allowed setting: scope, type, default and allowed values. A setting "
     "that is not here is refused (DPM01), so a typo cannot silently do nothing.",
     "scope <code>dataset</code>, key <code>silver_engine</code>, default <code>job</code>, allowed <code>job, declarative</code>"),
    ("config.domain", "One business area loaded as a unit. Holds its business timezone (what a day means), its "
     "environments, the whole manifest as loaded and the manifest's fingerprint.",
     "<code>tyre</code>, <code>Asia/Kolkata</code>, environments <code>dev, prod</code>"),
    ("config.domain_setting", "Settings for the whole domain (the platform-level configuration). Only values that "
     "differ from the registry default are stored.",
     "domain <code>tyre</code>, key <code>table_format</code>, value <code>iceberg-uc</code>"),
    ("config.source", "One system the data comes from, inside a domain.",
     "source id 2, domain <code>tyre</code>, name <code>erp</code>, environment <code>prod</code>"),
    ("config.dataset", "One table or feed of a source: identity, kind, how it is ingested, its history type, the "
     "contract it follows, and the manifest it came from.",
     "<code>sales_order_line</code>, enabled, history <code>scd2</code>, contract <code>sales_order_line</code> 1.2.0"),
    ("config.dataset_setting", "Settings for one dataset. They override the domain's value.",
     "dataset 14, key <code>silver_engine</code>, value <code>declarative</code>"),
    ("config.dataset_column", "The columns of a dataset, in order, from its contract: type, required flag, "
     "primary-key position and personal-data class.",
     "dataset 14, position 3, <code>dealer_id</code>, <code>string</code>, required"),
    ("config.business_key", "Which columns identify a row: the primary key for history and merge, and the dedup key "
     "for removing duplicates.", "dataset 14, kind <code>primary</code>, position 1, <code>order_id</code>"),
    ("config.dq_rule", "One data-quality rule of a dataset, written from the contract: the check, its severity and "
     "what happens when it fails (quarantine the rows, stop the load or warn).",
     "rule <code>qty_positive</code> on <code>quantity</code>, check <code>min</code> 1, severity <code>major</code>, action <code>quarantine</code>"),
]
RUNTIME_TABLES = [
    ("runtime.config_snapshot", "The exact merged configuration, stored once per fingerprint. Every dataset run points "
     "at one, so a past run is explained by the configuration it really used.",
     "fingerprint <code>4be1…</code>, the merged settings as JSON, first seen 20 Nov 06:00"),
    ("runtime.run", "One platform run: a trigger for a domain in an environment. Its status follows its dataset runs.",
     "domain <code>tyre</code>, env <code>dev</code>, trigger <code>schedule</code>, type <code>normal</code>, status <code>succeeded</code>"),
    ("runtime.dataset_run", "One dataset's part of a run: status, the configuration used, the watermark before and "
     "after, and row counts.",
     "dataset 14, <code>succeeded</code>, watermark 2026-11-19 to 2026-11-20, counts <code>rows_in 12840, quarantined 3</code>"),
    ("runtime.step_log", "One step inside a dataset run. The step names are the contract with observability.",
     "step <code>quality.sales_order_line</code>, <code>warned</code>, 3 rows quarantined"),
    ("runtime.file_log", "Every delivered file or batch: name, window, checksum, rows declared and rows read, "
     "outcome. The authority for what is already handled, and append-only.",
     "<code>sol_20261120.parquet</code>, declared 12843, read 12843, status <code>loaded</code>, load type <code>incremental</code>"),
    ("runtime.error_log", "A failure with its class, its step and a redacted message (secrets removed, capped at "
     "4000 characters).", "step <code>land</code>, class <code>ChecksumMismatch</code>, file changed since first delivery"),
    ("runtime.watermark", "The high-water mark of a dataset. It moves in the same transaction that closes a "
     "successful run, never before.", "dataset 14, value <code>2026-11-20</code>"),
    ("runtime.bookmark", "A named cursor, for when one watermark is not enough: a shard or a change-feed position.",
     "dataset 21, key <code>dms_feed</code>, value <code>00004812</code>"),
    ("runtime.watermark_audit", "Who moved a watermark by hand, from what to what and why. Append-only.",
     "dataset 14, <code>2026-11-20</code> to <code>2026-11-15</code>, reason: supplier resent a week"),
    ("runtime.backfill_plan", "A planned reprocessing: which windows, from which date and why.",
     "plan <code>BF-017</code>, window <code>2026-10</code>, active from 22 Nov, reason: wrong unit price"),
    ("runtime.correction", "One corrected value under a backfill plan. Silver is never edited: the correction is a "
     "row and gold is rebuilt from it.",
     "plan <code>BF-017</code>, order A-1002 line 1, field <code>unit_price</code>, value 41.50, was 14.50"),
    ("runtime.dq_result", "What each quality rule found in each run: counts only, never the failing rows.",
     "rule <code>qty_positive</code>, checked 12843, failed 3, action <code>quarantine</code>"),
    ("runtime.dq_profile", "A statistical profile of what a run loaded (counts, nulls, ranges). The newest one is what "
     "drift is compared against.", "rows 12840, <code>quantity</code> min 1, max 96, nulls 0"),
    ("runtime.batch_void", "A loaded batch declared not to count, with a reason and an approver. The file log is not "
     "touched.", "batch <code>erp/sales_order_line/2026-11-18</code>, reason: duplicate export, approved by the data owner"),
    ("runtime.table_lease", "A short lock so only one run writes a table at a time. A lease past its expiry (a crashed "
     "run) is taken over.", "table <code>silver.erp__sales_order_line</code>, run 7e3a…, expires 06:30"),
]


@page("meta-tables", "The 24 tables at a glance: purpose and an example row", parent="meta", short="All tables",
      blurb="Nine config tables say what to load. Fifteen runtime tables say what happened.")
def meta_tables():
    out = ("<p class='lead'>The store has 24 tables in two schemas. The <code>config</code> schema says what to load and "
           "how. The <code>runtime</code> schema says what happened. This page gives each table's purpose and one "
           "example row. The example values are made up in the shape of the tyre company's data. The table and column "
           "names are the real ones.</p>"
           "<p>Why so many? Key and value rows are right for flat settings. They are wrong for three kinds of fact: "
           "lists (columns, keys, rules), facts that must change together (a run closing and its watermark moving), "
           "and records that must never be edited (audits, voids, corrections). Each of those has its own typed "
           "table.</p>")
    out += "<h2>Config: what to load and how (9 tables)</h2>"
    out += table(["Table", "Purpose", "Example row"], [[f"<code>{n}</code>", p, e] for n, p, e in CONFIG_TABLES])
    out += "<h2>Runtime: what happened (15 tables)</h2>"
    out += table(["Table", "Purpose", "Example row"], [[f"<code>{n}</code>", p, e] for n, p, e in RUNTIME_TABLES])
    out += "<h2>How the tables point at each other</h2>"
    out += table(["From", "To"], [
        ["domain", "source, then dataset, then its settings, columns, business keys and quality rules"],
        ["run", "dataset run, then its steps, files and profile; errors and quality results hang off the run and the dataset"],
        ["dataset", "watermark, bookmarks, the watermark audit, backfill plans (with their corrections) and voided batches"],
    ])
    out += (f"<p>Column by column detail: {link('meta-model')} for <code>config</code> and {link('meta-runtime')} for "
            f"<code>runtime</code>. Settings and how a value is chosen: {link('meta-settings')}.</p>")
    return out


# ----------------------------------------------------------------------------------------------------------- model
def model_shape():
    d = box(10, 120, 140, 70, "domain", "tyre", "bx")
    d += box(10, 240, 160, 70, "domain_setting", "defaults", "bx")
    d += box(210, 120, 140, 70, "source", "erp, mes, ...", "bx")
    d += box(410, 120, 150, 70, "dataset", "erp.material", "solid")
    d += box(640, 10, 170, 56, "dataset_setting", "key and value", "bx")
    d += box(640, 86, 170, 56, "dataset_column", "from the contract", "bx")
    d += box(640, 162, 170, 56, "business_key", "primary, dedup", "bx")
    d += box(640, 238, 170, 56, "dq_rule", "from the contract", "bx")
    d += box(860, 10, 120, 56, "setting_def", "the dictionary", "bxs")
    d += arrow(150, 155, 208, 155, "has") + arrow(350, 155, 408, 155, "has")
    d += arrow(80, 190, 80, 238)
    d += arrow(560, 140, 638, 40) + arrow(560, 150, 638, 114) + arrow(560, 160, 638, 190) + arrow(560, 170, 638, 264)
    d += arrow(725, 162, 725, 144)
    d += arrow(810, 38, 858, 38)
    return d


@page("meta-model", "The config data model: every table and every column", parent="meta", short="Config data model",
      blurb="A domain has sources. A source has datasets. A dataset has settings, columns, business keys and quality rules.")
def meta_model():
    out = ("<p class='lead'>The <code>config</code> schema is the platform's description of what it must load. It "
           "has nine tables. Every pipeline decision that differs between datasets is read from them.</p>"
           "<p>The model is small on purpose. A dataset is one row with a handful of typed columns for the facts "
           "every pipeline needs, a key and value table for the many optional settings, and the whole manifest "
           "kept as a JSON document for everything else.</p>")
    out += fig(990, 330, model_shape(), "How the config tables relate",
               "Read it left to right. The highlighted box is the dataset, the centre of the model. A business key points "
               "at a column of the same dataset. A setting row points at the dictionary of allowed settings.")
    out += "<h2>Goes in, what happens, comes out</h2>"
    out += flow3("One document per domain and one per dataset, built by the loader from the YAML manifest and the "
                 "data contract.",
                 "The functions <code>config.load_domain</code> and <code>config.load_dataset</code> check the "
                 "document and replace the dataset's rows whole, in one transaction.",
                 "Rows in the nine tables, read back only through functions such as "
                 "<code>config.dataset_view</code>.")
    out += "<h2>A worked example: the materials dataset as rows</h2>"
    out += ("<p>The tyre company's material master comes from the manifest file "
            f"{src(TYRE + 'datasets/erp.material.yaml')}. After the loader runs, the store holds these rows for "
            "it. The first column is the table, the second is how many rows, the third is what they say.</p>")
    out += table(["Table", "Rows", "Content"], [
        ["<code>config.domain</code>", "1", "<code>tyre</code>, business time zone <code>Asia/Kolkata</code>, environments <code>dev</code> and <code>prod</code>."],
        ["<code>config.source</code>", "1", "<code>erp</code>, environment <code>prod</code>. The row is shared with the two other ERP datasets."],
        ["<code>config.dataset</code>", "1", "<code>material</code>, enabled, kind <code>dataset</code>, ingest mode <code>pull</code>, ingest read <code>full_copy</code>, history kind <code>master</code>, load pattern <code>history</code>, contract <code>tyre-erp-material</code> version <code>0.1.0</code>."],
        ["<code>config.dataset_setting</code>", "10", "The seven keys of the manifest's <code>config</code> block, such as <code>load_type = FULL</code>, and the three keys of its <code>dedup</code> block, such as <code>dedup.order_by = extracted_at</code>."],
        ["<code>config.dataset_column</code>", "6", "<code>material_code</code>, <code>description</code>, <code>compound_code</code>, <code>rim_size_in</code>, <code>tyre_class</code>, <code>extracted_at</code>, in the contract's order."],
        ["<code>config.business_key</code>", "2", "Kind <code>primary</code>, position 1, <code>material_code</code>. Kind <code>dedup</code>, position 1, <code>material_code</code>."],
        ["<code>config.dq_rule</code>", "One per quality rule in the contract", "The rules the contract declares on <code>rim_size_in</code> and <code>tyre_class</code>."],
    ])
    out += "<h2>The tables, one at a time</h2>"
    out += "<h3>config.setting_def: the dictionary of allowed settings</h3>"
    out += (f"<p>One row per setting that a manifest may use. {link('meta-settings')} explains it in full. Its "
            "primary key is <code>(scope, key)</code>, so the same key can exist at two scopes.</p>")
    out += cols([
        ("scope", "Text. One of platform, source, dataset.", "The level the setting belongs to."),
        ("key", "Text.", "The setting's name, for example <code>retry_count</code>."),
        ("value_type", "Text.", "The kind of value, for example <code>int</code>, <code>bool</code>, <code>enum</code>."),
        ("default_value", "Text, may be empty.", "The value used when nothing sets the key."),
        ("allowed", "Array of text, may be empty.", "The list of permitted values for an enum."),
        ("description", "Text.", "A sentence for people."),
        ("status", "Text. <code>registered</code> or <code>proposed</code>.", "Whether the key is settled or still a proposal from the specifications."),
    ])
    out += "<h3>config.domain: one row per domain</h3>"
    out += ("<p>A domain is one business area with its own manifest folder. The tyre company is one domain.</p>")
    out += cols([
        ("domain", "Text. Primary key.", "The domain's name."),
        ("business_timezone", "Text, required.", "The time zone in which a business day is counted."),
        ("environments", "Array of text.", "The environments the domain runs in."),
        ("manifest", "JSON, required.", "The whole <code>platform.yaml</code> as written, kept for reference."),
        ("fingerprint", "Text, required.", "A short hash of the manifest. It changes when the file changes."),
        ("loaded_at", "Timestamp.", "When the loader last wrote the row."),
    ])
    out += "<h3>config.domain_setting: defaults for a whole domain</h3>"
    out += cols([
        ("domain", "Text. Foreign key to <code>config.domain</code>. Deleted with its domain.", "The domain the default applies to."),
        ("scope", "Text.", "The scope under which the key was found in the dictionary: dataset or platform."),
        ("key", "Text.", "The setting's name."),
        ("value", "Text, required.", "The domain's value."),
    ])
    out += ("<p>The primary key is <code>(domain, key)</code>. The pair <code>(scope, key)</code> is a foreign key "
            "to <code>config.setting_def</code>, so a row cannot exist for a key the dictionary does not have.</p>")
    out += "<h3>config.source: one row per source system</h3>"
    out += code(SQL, r"^CREATE TABLE IF NOT EXISTS config\.source", caption="The whole table. It has four columns and nothing else.")
    out += cols([
        ("source_id", "Number assigned by the database. Primary key.", "The id datasets point at."),
        ("domain", "Text. Foreign key to <code>config.domain</code>.", "The domain the source belongs to."),
        ("source_name", "Text, unique across the whole store.", "The name used everywhere, for example <code>erp</code>."),
        ("source_env", "Text.", "Which environment of the source system this is, for example <code>prod</code>."),
    ])
    out += ("<p>A source row is not loaded on its own. <code>config.load_dataset</code> creates it the first time "
            "a dataset names the source. There is no place on this table for connection details or other "
            f"source-level settings. {link('meta-settings')} says what that means.</p>")
    out += "<h3>config.dataset: one row per dataset</h3>"
    out += code(SQL, r"^CREATE TABLE IF NOT EXISTS config\.dataset \(", caption="The centre of the model. The typed columns are the facts every pipeline asks for.")
    out += cols([
        ("dataset_id", "Number assigned by the database. Primary key.", "The id every other table points at."),
        ("source_id", "Foreign key to <code>config.source</code>.", "The source the dataset comes from."),
        ("dataset_name", "Text. Unique within its source.", "The dataset's name, for example <code>sales_order_line</code>."),
        ("is_enabled", "Boolean, required.", "Runs skip a dataset that is switched off."),
        ("kind", "Text, required.", "<code>dataset</code> for a source dataset. <code>gold_dimension</code> or <code>gold_fact</code> for a gold table, which sits in the same table. Ingestion and silver skip the gold kinds."),
        ("archetypes", "Array of text.", "The pipeline archetypes the manifest names, for example <code>PA2</code>."),
        ("ingest_mode", "Text.", "<code>push</code>, <code>pull</code> or <code>stream</code>."),
        ("ingest_read", "Text.", f"How the source is read. With the mode it picks the ingestion pipeline. See {link('ingest-pipelines')}."),
        ("history_kind", "Text.", "<code>master</code>, <code>transaction</code>, <code>event</code> or <code>snapshot</code>. Silver picks the history type from it."),
        ("load_pattern", "Text.", "The manifest's <code>load.pattern</code>."),
        ("contract_id, contract_version", "Text, required.", "The data contract the columns were taken from."),
        ("manifest", "JSON, required.", "The whole dataset manifest. Pipelines read the <code>load</code>, <code>volume</code> and <code>history</code> blocks from here."),
        ("fingerprint", "Text, required.", "A short hash of the manifest and the contract together."),
        ("loaded_at", "Timestamp.", "When the loader last wrote the row."),
    ])
    out += "<h3>config.dataset_setting: a dataset's own setting values</h3>"
    out += cols([
        ("dataset_id", "Foreign key to <code>config.dataset</code>. Deleted with its dataset.", "The dataset."),
        ("scope", "Text. Always <code>dataset</code>, enforced by a check.", "Lets the row point at the dictionary."),
        ("key", "Text.", "The setting's name."),
        ("value", "Text, required.", "The value. Every value is stored as text, whatever its type."),
    ])
    out += ("<p>The primary key is <code>(dataset_id, key)</code>. The pair <code>(scope, key)</code> is a foreign "
            "key to <code>config.setting_def</code>.</p>")
    out += "<h3>config.dataset_column: the contract's columns</h3>"
    out += cols([
        ("dataset_id", "Foreign key to <code>config.dataset</code>. Deleted with its dataset.", "The dataset."),
        ("ordinal", "Whole number. Primary key with the dataset id.", "The column's position in the contract."),
        ("column_name", "Text. Unique within the dataset.", "The column's name."),
        ("logical_type", "Text, required.", "The contract's type: string, integer, number, timestamp, date or boolean."),
        ("is_required", "Boolean.", "Whether the contract says the column must have a value."),
        ("primary_key_position", "Whole number, may be empty.", "The column's place in the primary key, if it is part of it."),
        ("description", "Text.", "The contract's description."),
        ("classification", "Text.", "The contract's personal-data class. A column classed <code>restricted</code> is replaced by a token before bronze."),
    ])
    out += "<h3>config.business_key: which columns identify a row</h3>"
    out += code(SQL, r"^CREATE TABLE IF NOT EXISTS config\.business_key", caption="A key column must be a column of the same dataset. The foreign key enforces it.")
    out += cols([
        ("dataset_id", "Whole number.", "The dataset."),
        ("key_kind", "Text. <code>primary</code> or <code>dedup</code>.", "The primary key identifies a row. The dedup key is the set of columns used to remove duplicates."),
        ("ordinal", "Whole number.", "The column's position within the key."),
        ("column_name", "Text. Foreign key, with the dataset id, to <code>config.dataset_column</code>.", "The column."),
    ])
    out += "<h3>config.dq_rule: a dataset's quality rules</h3>"
    out += cols([
        ("dataset_id", "Foreign key to <code>config.dataset</code>.", "The dataset."),
        ("rule_id", "Text. Primary key with the dataset id.", "Built by the loader as column, check and a counter, for example <code>ordered_qty-sql-1</code>."),
        ("column_name", "Text, may be empty.", "The column the rule is on. Empty for a rule on the whole row or batch."),
        ("check_name", "Text.", "The name of the check."),
        ("params", "JSON.", "The check's own fields from the contract."),
        ("severity", "Text. critical, major or minor.", "How bad a failure is."),
        ("action", "Text. quarantine, stop or warn.", "What a failure causes. The loader derives it from the severity: critical stops, major quarantines, minor warns."),
        ("enabled", "Boolean.", "Whether the rule runs."),
    ])
    out += "<h2>The settings this part reads</h2>"
    out += ("<p>The model itself reads no setting. It is where settings are kept. The typed columns of "
            "<code>config.dataset</code> are filled from these manifest fields.</p>")
    out += table(["Column", "Manifest field", "Values in the tyre manifests"], [
        ["<code>ingest_mode</code>", "<code>ingest.mode</code>", "push, pull, stream"],
        ["<code>ingest_read</code>", "<code>ingest.read</code>", "change_feed, files, full_copy, pages, events"],
        ["<code>history_kind</code>", "<code>history.kind</code>", "master, transaction, event, snapshot"],
        ["<code>load_pattern</code>", "<code>load.pattern</code>", "history, among others"],
        ["<code>kind</code>", "<code>kind</code>", "dataset, gold_dimension, gold_fact"],
    ])
    out += "<h2>The code that writes a dataset</h2>"
    out += code(SQL, r"    -- the dataset's settings, columns and keys are replaced whole", r"    RETURN ds;",
                "The second half of config.load_dataset. The dataset's child rows are deleted and written again in the same transaction.")
    out += ("<p>Walk through it. The three deletes clear the dataset's keys, columns and settings. The order "
            "matters: keys go first because they point at columns. The settings are inserted one row per key. "
            "The columns are inserted in one statement, and <code>WITH ORDINALITY</code> numbers them in the "
            "order the document lists them, which is the contract's order. The keys are inserted last with a "
            "counter per kind. Because the function is one transaction, a reader never sees a dataset with half "
            "its columns.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>The contract table in {src(T_STORE)} has cases for the model: columns come back in contract "
            "order, keys come back in position order, a business key that names a missing column is refused, and "
            "loading a dataset again leaves one row and replaces its settings whole.</p>")
    out += code(T_STORE, r'    \("load: a reload replaces settings whole"', 6,
                "Two real cases. After a reload with fewer settings, the dropped key falls back to its default.")
    out += not_built([
        "A source name is unique across the whole store, not within a domain. If a second domain loaded a dataset "
        "under a source name that already exists, the existing source row would be reused and would keep the "
        "first domain. Only one domain is loaded today, so this has not happened.",
        "A dataset that is removed from the manifest is not removed from the store. The loader reports it and "
        "leaves the rows. There is no function that deletes a dataset.",
        "<code>config.business_key</code> and <code>config.dq_rule</code> do not cascade from the dataset in the "
        "same way as the other child tables. Since no function deletes a dataset, nothing depends on it yet.",
        "Gold tables are stored as datasets under a source named <code>gold_</code> plus the gold manifest's "
        "domain, for example <code>gold_sales</code>. The word source is stretched to cover them.",
        "The store does not check a value against the dictionary's type or allowed list. The loader does, and "
        f"only as a warning. See {link('meta-settings')}.",
    ])
    return out


# -------------------------------------------------------------------------------------------------------- settings
def merge_shape():
    d = box(10, 30, 230, 70, "Registry default", "config.setting_def", "bxs")
    d += box(10, 130, 230, 70, "Domain value", "config.domain_setting", "bx")
    d += box(10, 230, 230, 70, "Dataset value", "config.dataset_setting", "bx")
    d += box(350, 110, 250, 110, "config.dataset_settings", ["the narrowest value", "that exists wins"], "solid")
    d += box(710, 130, 250, 70, "key, value, origin", "one row per dataset key", "bx")
    d += arrow(240, 65, 348, 140, "3rd") + arrow(240, 165, 348, 165, "2nd") + arrow(240, 265, 348, 195, "1st")
    d += arrow(600, 165, 708, 165)
    return d


@page("meta-settings", "Settings: the dictionary, the three levels, and how a value is chosen", parent="meta",
      short="Settings and the merge",
      blurb="A setting must be in the dictionary. Its value is the dataset's, else the domain's, else the default, and the origin is reported.")
def meta_settings():
    out = ("<p class='lead'>A setting is one named value that changes how a dataset is handled, such as "
           "<code>retry_count</code> or <code>watermark_column</code>. The store keeps a dictionary of every "
           "setting that is allowed, and it works out the final value of each one for each dataset.</p>"
           "<p>The dictionary exists because of typing mistakes. A manifest that says <code>retyr_count: 5</code> "
           "would otherwise be accepted, ignored, and the dataset would quietly run with the default. Here it is "
           "refused before anything is written.</p>")
    out += fig(980, 330, merge_shape(), "Three places a value can come from and the function that picks one",
               "The function looks at the dataset's own value first, then the domain's, then the default. It "
               "reports which of the three it used.")
    out += "<h2>The dictionary, counted</h2>"
    out += (f"<p>The dictionary is the file {src(REGJSON)}. It is generated by a script from an older ingestion "
            "framework's key list plus the keys the specifications propose, and it must not be edited by hand. "
            "I counted its entries for this page.</p>")
    out += table(["Scope", "Entries", "Of which proposed", "What the scope is meant for"], [
        ["dataset", "135", "21", "A value that can differ per dataset."],
        ["source", "43", "0", "A value that belongs to a source system, such as a server name or an authentication method."],
        ["platform", "35", "0", "A value for the whole platform, such as a default schedule."],
        ["Total", "213", "21", ""],
    ])
    out += ("<p>Three details in the count matter. The source-scope key <code>client_id</code> appears twice in "
            "the file, so the table <code>config.setting_def</code>, whose primary key is scope and key, ends "
            "with 212 rows. Ten keys are registered at both platform scope and dataset scope: the eight "
            "<code>processor_groups</code> switches and the two lifecycle day counts. Of the 213 entries, 60 "
            "carry a default and 46 carry a list of allowed values.</p>")
    out += code(REGJSON, r'"default": "job",', 6,
                "One entry as the file holds it. The lines just above this cut list the two allowed values, job and declarative.")
    out += table(["Field", "What it holds"], [
        ["<code>scope</code>", "platform, source or dataset."],
        ["<code>value_type</code>", "The file uses ten types. The most common are string with 98 entries, enum with 45, int with 34 and bool with 12."],
        ["<code>default</code>", "The value when nothing sets the key, or nothing at all."],
        ["<code>allowed</code>", "The permitted values of an enum."],
        ["<code>status</code>", "<code>registered</code> for a settled key, <code>proposed</code> for one the specifications ask for. Both are accepted by the loader. The status changes nothing at run time."],
    ])
    out += code(SQL, r"^CREATE TABLE IF NOT EXISTS config\.setting_def", caption="The dictionary as a table.")
    out += "<h2>Where a value can be set</h2>"
    out += table(["Level", "Written in", "Stored in", "Wins over"], [
        ["Default", "The dictionary file", "<code>config.setting_def.default_value</code>", "Nothing"],
        ["Domain", "The <code>settings</code> block of <code>platform.yaml</code>", "<code>config.domain_setting</code>", "The default"],
        ["Dataset", "The <code>config</code> and <code>dedup</code> blocks of a dataset manifest", "<code>config.dataset_setting</code>", "The domain value and the default"],
    ])
    out += "<h2>How the final value is chosen</h2>"
    out += code(SQL, r"^-- The merged view \(DPC-0002\)", END, "The whole merge is one query.")
    out += ("<p>Walk through it. The query starts from the dictionary and keeps only the rows whose scope is "
            "<code>dataset</code>. For each such key it looks for a row in <code>config.dataset_setting</code> "
            "for this dataset, and for a row in <code>config.domain_setting</code> for the dataset's domain. "
            "<code>COALESCE</code> returns the first of the three values that exists: dataset, domain, default. "
            "The <code>CASE</code> beside it names which one that was. The result has one row for every dataset "
            "key in the dictionary, including keys nobody set and that have no default. Their value is empty "
            "and their origin is <code>default</code>.</p>")
    out += "<h2>A worked example</h2>"
    out += ("<p>These are four keys of the tyre company's sales order lines and its dealers, as the merge returns "
            "them. The columns are the dataset, the key, the final value and where the value came from.</p>")
    out += table(["Dataset", "Key", "Value", "Origin"], [
        ["<code>erp.sales_order_line</code>", "<code>watermark_column</code>", "<code>change_seq</code>", "dataset"],
        ["<code>erp.sales_order_line</code>", "<code>retry_count</code>", "<code>2</code>", "default"],
        ["<code>dealer_api.dealer</code>", "<code>retry_count</code>", "<code>3</code>", "dataset"],
        ["<code>erp.delivery</code>", "<code>csv_has_header</code>", "<code>true</code>", "default"],
    ])
    out += (f"<p>No row in this example has the origin <code>domain</code>. The tyre company's "
            f"{src(TYRE + 'platform.yaml')} has no <code>settings</code> block, so the domain level is empty for "
            "it. The domain level is exercised by the contract test, which loads a small made-up domain with "
            "one domain setting.</p>")
    out += "<h2>The view the pipelines read</h2>"
    out += ("<p>No pipeline calls the merge directly. Pipelines call <code>config.dataset_view</code>, which "
            "returns one JSON document with everything about a dataset: its identity, the merged settings, the "
            "origin of each, the columns, the keys and the manifest.</p>")
    out += code(SQL, r"^CREATE OR REPLACE FUNCTION config\.dataset_view", END, "The view is built in one statement.", cap=40)
    out += ("<p>Two lines are worth a second look. The <code>settings</code> object keeps only the keys that "
            "have a value, so a key with no default that nobody set is absent from the view. The "
            "<code>origins</code> object has the same keys and says where each value came from. The document "
            "also carries the dataset's fingerprint, which the run record stores.</p>")
    out += "<h2>Refusing an unknown key</h2>"
    out += code(SQL, r"    FOR k IN SELECT jsonb_object_keys\(COALESCE\(p_doc -> 'settings'", 5,
                "config.load_dataset: a key that is not a dataset-scope key in the dictionary stops the load with DPM01.")
    out += code(REGISTRY, r"^def type_problem\(", caption="The loader's check of a value against its type. A problem here is a warning, not a refusal.")
    out += "<h2>Two gaps to know about</h2>"
    out += note("Gap 1: source-scope settings have nowhere to live",
                "<p>The dictionary registers 43 source-scope settings. The table <code>config.source</code> has "
                "four columns: an id, the domain, the name and the environment. There is no "
                "<code>source_setting</code> table, no function that stores a value for a source-scope key, and "
                "no manifest file for a source. A source-scope key written into a dataset manifest is refused, "
                "because the load accepts only dataset-scope keys. The merge has three levels in the "
                "specification and two in the code. The Python port says so itself.</p>"
                + code(PORT, r"    def settings\(self, source: str, dataset: str\)", 3,
                       "The port's own note on the missing level."))
    out += note("Gap 2: platform-scope values can be stored but are never returned",
                # shiftleft: allow SL-SEC-01 -- prose inside an HTML note, not a SQL statement
                "<p><code>config.load_domain</code> accepts a key in <code>platform.yaml</code> when the "
                "dictionary has it at dataset scope or at platform scope, and stores it in "
                "<code>config.domain_setting</code> with the scope it found. The merge keeps only dictionary "
                "rows whose scope is <code>dataset</code>. A platform-scope value therefore sits in the table "
                "and no function returns it. For the ten keys that exist at both scopes the load picks the "
                "dataset scope, so those do take part in the merge.</p>"
                + code(SQL, r"    FOR k, v IN SELECT \* FROM jsonb_each_text\(COALESCE\(p_doc -> 'settings', '\{\}'\)\) LOOP", 8,
                       "config.load_domain: the scope is looked up among dataset and platform, and dataset sorts first."))
    out += "<h2>Which settings the pipelines really read</h2>"
    out += ("<p>The dictionary is much larger than what the code uses, because it was carried over from an older "
            "framework for a different cloud. Many of its keys describe connectors this platform does not have. "
            "The pages for each layer list the keys their code reads. For ingestion they are the file format "
            "keys, the three change feed column keys, <code>file_pattern</code>, <code>watermark_column</code> "
            f"and the two retry keys. See {link('ingest-steps')} and {link('ingest-pipelines')}.</p>")
    out += "<h2>How it is tested</h2>"
    out += code(T_STORE, r'    \("merge: the dataset value wins"', 7,
                "Four real cases from the contract table: each level winning in turn, and platform keys left out of the merge.")
    out += (f"<p>{src(T_LOADER)} adds two things. One test checks each kind of type problem. Another rebuilds "
            "the dictionary with the export script and checks that the file in the repository is exactly what "
            "the script produces.</p>")
    out += code(T_LOADER, r'        \("load_type", "INCREMENTAL", None\)', 7, "The type check's cases: a value, and the problem reported or none.")
    out += not_built([
        "Source-scope settings cannot be stored or merged. See gap 1 above.",
        "Platform-scope values stored at domain level are not returned by any function. See gap 2 above.",
        "A value that does not fit its type or its allowed list is reported as a warning by the loader and is "
        "still written. The store keeps every value as text and does not check it.",
        "The functions <code>config.dataset_settings</code> and <code>config.settings_registry</code> have no "
        "caller in the platform code that I could find. Pipelines use <code>config.dataset_view</code>. Only "
        "the tests call the other two.",
        "The duplicate <code>client_id</code> entry in the dictionary file is silently merged into one row.",
        "The <code>quality_defaults</code> block of <code>platform.yaml</code> is not a setting. It is kept "
        "inside the domain's manifest JSON and no function reads it back.",
    ])
    return out


# ------------------------------------------------------------------------------------------------------- functions
def fn_table(rows):
    return table(["Function", "What it does", "What it refuses", "Who calls it"],
                 [[f"<code>{a}</code>", b, c, d] for a, b, c, d in rows])


@page("meta-functions", "Every function in the store, grouped by purpose", parent="meta", short="Functions",
      blurb="Thirty-eight functions in the main file and one in the operations file. What each does, refuses, and who calls it.")
def meta_functions():
    out = ("<p class='lead'>The functions are the store's whole public surface. This page lists every one. The "
           f"main file {src(SQL)} defines 38. The operations file {src(OBS)} adds one more.</p>"
           "<p>Reading them as a list is the fastest way to learn what the platform can and cannot do with its "
           "metadata. If an action has no function, no pipeline can perform it.</p>")
    d = box(10, 20, 200, 70, "Loader", "writes configuration", "bx", href="meta-loader.html")
    d += box(10, 120, 200, 70, "Ingestion", "runs, files, watermarks", "bx", href="ingest-runner.html")
    d += box(10, 220, 200, 70, "Silver and gold", "runs, bookmarks, leases", "bx", href="silver.html")
    d += box(330, 100, 240, 110, "PostgresMetaStore", ["one Python method", "per function"], "solid")
    d += box(690, 40, 270, 80, "config functions", "12", "bxs")
    d += box(690, 190, 270, 80, "runtime functions", "26, and 1 for operations", "bxs")
    d += arrow(210, 55, 328, 135) + arrow(210, 155, 328, 155) + arrow(210, 255, 328, 180)
    d += arrow(570, 140, 688, 80) + arrow(570, 170, 688, 230)
    out += fig(980, 310, d, "Callers, the adapter and the two groups of functions",
               "Every caller holds the same Python object. Each of its methods is one SELECT of one function.")
    out += "<h2>What every function has in common</h2>"
    out += code(SQL, r"^CREATE OR REPLACE FUNCTION config\._dataset_id", END,
                "The helper nearly every function starts with. It turns a source and dataset name into an id, or refuses with DPM02.")
    out += ("<p>Three things in the second line apply to all of them. <code>SECURITY DEFINER</code> makes the "
            "function run with its owner's rights, which is how a caller with no right on any table can still "
            "read and write through it. <code>SET search_path</code> pins the schema search to the system "
            "catalogue, so a caller cannot make the function pick up a table of the caller's own. Functions that "
            "only read are marked <code>STABLE</code>.</p>")
    out += "<h2>Group 1: writing configuration</h2>"
    out += fn_table([
        ("config.register_settings(defs)", "Writes the dictionary of allowed settings. An entry that exists is updated.", "A scope that is not platform, source or dataset.", "The loader."),
        ("config.load_domain(doc)", "Writes one domain row and replaces its domain settings.", "A setting key that is not in the dictionary at dataset or platform scope.", "The loader."),
        ("config.load_dataset(doc)", "Writes one dataset: creates the source row if needed, then replaces the dataset's settings, columns and keys whole.", "A domain that is not loaded. A key that is not a dataset-scope setting. A business key kind other than primary or dedup. A business key that names a column the dataset does not have.", "The loader, for source datasets and for gold tables."),
        ("config.set_dq_rules(source, dataset, rules)", "Replaces a dataset's quality rules. A rule that already has logged results is kept and updated, never dropped.", "A rule id twice. An empty rule id. A column the dataset does not have. An unknown severity or action.", "The loader."),
    ])
    out += "<h2>Group 2: reading configuration</h2>"
    out += fn_table([
        ("config.dataset_view(source, dataset)", "Returns one JSON document with everything about a dataset: identity, merged settings and their origins, columns, keys, manifest, fingerprint.", "A dataset that is not loaded.", "Ingestion, silver, gold, housekeeping, retention and freshness. This is the function pipelines live on."),
        ("config.list_datasets(domain)", "Lists a domain's datasets with enabled flag, kind, ingest mode and fingerprint, in name order.", "Nothing. An unknown domain gives an empty list.", "Every runner, to decide what to run."),
        ("config.dataset_settings(source, dataset)", "Returns every dataset-scope key with its final value and origin.", "A dataset that is not loaded.", "<code>config.dataset_view</code> internally. No direct caller in the platform code that I found."),
        ("config.dataset_columns(source, dataset)", "Returns the contract's columns in order.", "A dataset that is not loaded.", "Gold."),
        ("config.business_keys(source, dataset, kind)", "Returns the columns of the primary or the dedup key, in position order.", "A kind other than primary or dedup.", "Gold."),
        ("config.dq_rules(source, dataset)", "Returns the dataset's quality rules by rule id.", "A dataset that is not loaded.", "Silver."),
        ("config.settings_registry()", "Returns the whole dictionary.", "Nothing.", "Only the tests, as far as I found."),
        ("config._dataset_id(source, dataset)", "Helper. Finds the dataset's id.", "A dataset that is not loaded.", "Other functions."),
    ])
    out += "<h2>Group 3: opening and closing runs</h2>"
    out += fn_table([
        ("runtime.start_run(run_id, domain, env, trigger, run_type)", "Opens a run with status running.", "An unknown run type. A domain that is not loaded. A run id that was already used.", "Every runner, first thing."),
        ("runtime.start_dataset_run(run_id, source, dataset, fingerprint, config)", "Opens a dataset's record inside a run, saves the configuration snapshot, and returns the watermark as it stands.", "A run that is finished. A dataset started twice in the same run.", "Every runner, once per dataset."),
        ("runtime.finish_dataset_run(run_id, source, dataset, status, counts, watermark, bookmarks, message)", "Closes the dataset's record. Moves the watermark and bookmarks only when the status is succeeded.", "A status other than succeeded, failed or skipped. A dataset not started in this run. A dataset run already closed.", "Every runner, and the lease helper when a table is busy."),
        ("runtime.finish_run(run_id)", "Closes the run and works out its status from its dataset runs.", "A run already closed. A run that still has a dataset running.", "Every runner, last thing."),
        ("runtime._check_run, runtime._dataset_run", "Helpers. The first checks the run exists. The second checks the dataset was started in the run.", "<code>DPM02</code> and <code>DPM04</code>.", "Other functions."),
    ])
    out += code(SQL, r"^CREATE OR REPLACE FUNCTION runtime\.finish_run", END,
                "How a run's status follows its datasets: none failed is succeeded, all failed is failed, anything between is partial, and a run with no dataset is failed.")
    out += "<h2>Group 4: logging what happened</h2>"
    out += fn_table([
        ("runtime.log_step(...)", "Appends one step row with a status, counts and a message.", "An unknown step status. A dataset not started in this run.", "Every runner and the lease helper."),
        ("runtime.log_file(...)", "Appends one file log row for a batch, whatever its outcome.", "A status that is not one of the five lander outcomes. An unknown load type. A dataset not started in this run.", "The ingestion steps, and silver's baseline writer."),
        ("runtime.log_error(...)", "Appends one error row. The dataset may be empty for an error that belongs to the whole run.", "A run that was never started.", "Ingestion, silver and gold."),
        ("runtime.log_dq_results(rows)", "Appends what each quality rule found, all rows or none.", "A rule the dataset does not have. More failed than checked. An unknown action.", "No caller in the platform code that I found. Only the tests."),
        ("runtime.save_profile(run_id, source, dataset, profile)", "Keeps a dataset run's profile, which is a summary of its rows and columns.", "A dataset not started in this run.", "The ingestion runner."),
        ("runtime._redact(message)", "Helper. Replaces secrets in a message and cuts it to 4000 characters.", "Nothing.", "Every function that stores a message."),
    ])
    out += "<h2>Group 5: watermarks, bookmarks and batches</h2>"
    out += fn_table([
        ("runtime.get_watermark(source, dataset)", "Returns the dataset's watermark, which is the highest change value already taken.", "A dataset that is not loaded.", "The ingestion pipelines."),
        ("runtime.get_bookmarks(source, dataset)", "Returns the dataset's named positions, such as a page cursor or a stream position.", "A dataset that is not loaded.", "Ingestion, silver, gold and freshness."),
        ("runtime.reset_watermark(source, dataset, value, reason, changed_by)", "Sets the watermark by hand and writes an audit row with the old value, the new value, who and why.", "A reason shorter than 10 characters. No name of who made it.", "No caller in the platform code that I found. It is there for an operator."),
        ("runtime.batch_status(source, dataset, batch_key)", "Returns the latest status of a batch, or <code>void</code> if it was voided, or nothing if it was never seen.", "A dataset that is not loaded.", "The ingestion steps, to skip a batch already loaded."),
        ("runtime.dataset_files(source, dataset, window)", "Returns every file log row of a dataset, oldest first, with voided batches shown as void.", "A dataset that is not loaded.", "Ingestion, silver, gold, housekeeping and retention."),
        ("runtime.void_batch(source, dataset, batch_key, reason, approved_by)", "Marks a loaded batch as void, so every later read skips it.", "A batch that never loaded. A batch already void. No approver. A reason shorter than 10 characters.", "The recovery module and housekeeping."),
    ])
    out += code(SQL, r"^CREATE OR REPLACE FUNCTION runtime\.batch_status", END,
                "A void wins over whatever the file log says. Otherwise the newest file log row of the batch key decides.")
    out += "<h2>Group 6: corrections and backfill plans</h2>"
    out += fn_table([
        ("runtime.record_corrections(plan, rows)", "Records a backfill plan and its corrections together. A correction says an old value was wrong. It is a row here and never an edit of silver.", "A plan id seen before. No reason or no author. No windows. A key that is not the dataset's primary key. A field that is not a column.", "The recovery module, and the simulated company when its sales team corrects a date."),
        ("runtime.corrections(source, dataset, as_of)", "Lists a dataset's corrections, each marked active once its plan's date is reached.", "A dataset that is not loaded.", "Gold."),
        ("runtime.backfill_plans(source, dataset)", "Lists a dataset's plans with the windows each one rebuilds.", "A dataset that is not loaded.", "Gold."),
    ])
    out += "<h2>Group 7: leases</h2>"
    out += fn_table([
        ("runtime.acquire_lease(table, run_id, ttl_seconds)", "Takes the writer lease on a table. Returns nothing when this run holds it, otherwise the run that does.", "A time limit that is zero or negative.", "Ingestion, silver, gold and housekeeping, before they write a table."),
        ("runtime.release_lease(table, run_id)", "Gives the lease back. Only the holder's call has any effect.", "Nothing.", "The same callers, when they are done."),
    ])
    out += "<h2>Group 8: reading the record back</h2>"
    out += fn_table([
        ("runtime.run_report(run_id)", "Returns everything recorded for one run as one JSON document: the run, its datasets, steps, files and errors.", "A run that was never started.", "Gold's inspection tool and the platform script."),
        ("runtime.dq_results(run_id, source, dataset)", "Returns the quality results of a run.", "A run that was never started.", "Only the tests, as far as I found."),
        ("runtime.last_profile(source, dataset)", "Returns the newest profile kept for the dataset.", "A dataset that is not loaded.", "The ingestion runner, to compare with the new profile."),
        ("runtime.ops_run_log(domain, since, until)", "Returns every run of a domain in a time span with its datasets, steps, files and errors. It is in the operations file.", "Nothing.", f"The run log reader behind the operations page. See {link('obs-runlog')}."),
    ])
    out += "<h2>The adapter: one method per function</h2>"
    out += code(PG, r"    def finish_dataset_run\(self, run_id", caption="A typical method. It holds no table name and no rule. It passes its arguments to the function.")
    out += ("<p>The connection is set to commit each statement on its own, so one function call is one "
            "transaction. That is what makes a function such as <code>finish_dataset_run</code> all or nothing.</p>")
    out += "<h2>A worked example: the calls for one dataset in one ingestion run</h2>"
    out += ("<p>The table shows the order in which the ingestion run for the tyre company's material master "
            "calls the store. The left column is the function. The right column is what it is for at that "
            "moment.</p>")
    out += table(["Function", "Purpose at that moment"], [
        ["<code>runtime.start_run</code>", "Open the run."],
        ["<code>config.list_datasets</code>", "Find the enabled datasets of the domain."],
        ["<code>config.dataset_view</code>", "Read the material dataset's merged configuration."],
        ["<code>runtime.start_dataset_run</code>", "Open the dataset's record and save the configuration snapshot."],
        ["<code>runtime.acquire_lease</code>", "Make sure no other run is writing the bronze table."],
        ["<code>runtime.get_bookmarks</code>, <code>runtime.dataset_files</code>", "Find which full copies were already taken."],
        ["<code>runtime.batch_status</code>", "Check the new copy was not loaded before."],
        ["<code>runtime.log_file</code>", "Record the batch and its outcome."],
        ["<code>runtime.log_step</code>, <code>runtime.save_profile</code>", "Record the counts and the profile."],
        ["<code>runtime.finish_dataset_run</code>", "Close the record and move the bookmarks if it succeeded."],
        ["<code>runtime.release_lease</code>, <code>runtime.finish_run</code>", "Give the lease back and close the run."],
    ])
    out += f"<p>{link('ingest-runner')} walks through the same sequence in the Python code.</p>"
    out += "<h2>The settings these functions read</h2>"
    out += ("<p>The functions read no setting. Their limits are constants in the SQL: a reason must have at "
            "least 10 characters, a stored message is cut at 4000 characters, and a lease needs a positive time "
            f"limit. The same constants are in {src(PORT)} so the in-memory adapter behaves the same way.</p>")
    out += "<h2>How it is tested</h2>"
    out += (f"<p>Each function has cases in the contract table of {src(T_STORE)}, both for what it returns and "
            "for each refusal. The same cases run against the in-memory adapter, which is how the two are kept "
            "in step.</p>")
    out += code(T_STORE, r'    \("run: same run id twice refused"', 3, "Two refusal cases as the table writes them: an action and the exception expected.")
    out += not_built([
        "Three functions have no caller in the platform code that I could find: "
        "<code>runtime.reset_watermark</code>, <code>runtime.log_dq_results</code> and "
        "<code>runtime.dq_results</code>. They are tested but not yet used by a pipeline or a tool. The table "
        "<code>runtime.dq_result</code> therefore stays empty in a normal run.",
        "There is no function to reset or delete a bookmark, to delete a dataset, or to read the domain row.",
        "There is no function that lists runs for a pipeline to inspect. The only reader across runs is "
        "<code>runtime.ops_run_log</code> in the operations file.",
        "My list of callers comes from searching the source for each method name. A caller that reaches the "
        "store through a wrapper with a different name could be missing from it.",
    ])
    return out


# ---------------------------------------------------------------------------------------------------------- loader
@page("meta-loader", "The loader: from YAML manifests in git to rows in the store", parent="meta", short="Loader",
      blurb="Reads a manifest folder, checks everything, and only then writes. A bad manifest changes nothing.")
def meta_loader():
    out = ("<p class='lead'>People describe a domain in YAML files kept in git. The loader is the one piece of "
           "code that reads those files and writes the configuration into the metadata store.</p>"
           "<p>It exists so that the running platform never depends on a file. Pipelines read the store. The "
           "loader runs on a laptop or in a build, validates the whole folder, and refuses to write anything if "
           "one file is wrong.</p>")
    d = box(10, 30, 180, 70, "platform.yaml", "one per domain", "bx")
    d += box(10, 130, 180, 70, "datasets/*.yaml", "one per dataset", "bx")
    d += box(10, 230, 180, 70, "contracts/*.yaml", "columns and rules", "bx")
    d += box(290, 110, 200, 110, "compile_domain", ["read, validate,", "build documents"], "solid")
    d += box(590, 30, 170, 70, "Errors", "stop, nothing written", "bxm")
    d += box(590, 230, 170, 70, "load", "writes in order", "bx")
    d += box(840, 230, 140, 70, "The store", "config schema", "bxs", href="meta-model.html")
    d += arrow(190, 65, 288, 140) + arrow(190, 165, 288, 165) + arrow(190, 265, 288, 190)
    d += arrow(490, 140, 588, 70, "any error") + arrow(490, 190, 588, 260, "none")
    d += arrow(760, 265, 838, 265)
    out += fig(990, 330, d, "The loader's two phases",
               "Everything on the left is read and checked first. The store is touched only on the lower path.")
    out += "<h2>The manifest folder</h2>"
    out += table(["Path under the domain's folder", "What it is", "Read by this loader"], [
        ["<code>manifest/platform.yaml</code>", "What applies to the whole domain: name, time zone, environments, optional settings.", "Yes, by <code>compile_domain</code>."],
        ["<code>manifest/datasets/*.yaml</code>", "One file per source dataset, named source, dot, dataset.", "Yes, by <code>compile_domain</code>."],
        ["<code>contracts/*.odcs.yaml</code>", "The data contract of each dataset: columns, types, keys, quality rules. Each manifest points at its contract with a relative path.", "Yes, through the manifest's <code>contract</code> field."],
        ["<code>manifest/gold/*.yaml</code>", "One file per gold table.", "Yes, by <code>load_gold</code>."],
        ["<code>manifest/metrics/</code>, <code>manifest/models/</code>", "Metric and model definitions.", "No. This loader does not open them."],
    ])
    out += code(TYRE + "platform.yaml", r"^kind: platform", r"^  minor:",
                "The tyre company's platform manifest, down to its quality defaults. It names no cloud and no product.", cap=12)
    out += code(TYRE + "datasets/erp.material.yaml", r"^source_name: erp", r"mass_delete_max_share",
                "The material master's dataset manifest, without its last blocks on volume, reconciliation and owners.", cap=40)
    out += "<h2>The steps, in order</h2>"
    out += ladder([
        ("Read the dictionary", "The list of allowed settings is read from the registry file."),
        ("Compile the domain", "The platform file becomes one domain document with a fingerprint."),
        ("Compile each dataset", "Each dataset manifest is read with its contract, validated, and turned into one dataset document."),
        ("Collect every problem", "Errors from all files are gathered. If there is at least one, the loader raises one error that lists them all and stops. Nothing has been written."),
        ("Add binding settings", "Optional extra settings from the deployment are laid over a dataset's own, and its fingerprint is recomputed to cover them."),
        ("Write", "The dictionary is registered, then the domain, then each dataset followed by its quality rules."),
        ("Report", "The loader returns what it loaded, any warnings, and any dataset the store has that the folder no longer declares."),
    ])
    out += "<h2>Building a dataset document</h2>"
    out += code(LOADER, r"^def dataset_doc\(", caption="One dataset as the store's load function wants it.")
    out += ("<p>The document mixes three origins. The identity and the typed fields come from the manifest. The "
            "columns and the primary key come from the contract. The fingerprint is a hash of the manifest and "
            "the contract together, so a change in either gives a new fingerprint. The manifest is also stored "
            "whole, because the pipelines read its <code>load</code> and <code>volume</code> blocks straight from "
            "it.</p>")
    out += code(LOADER, r"^def settings_of\(", caption="Which parts of a manifest become settings: the config block as it is, and three keys of the dedup block with a dedup prefix.")
    out += ("<p>Every value is turned into text. A list becomes its items joined by commas, which is why the "
            "sales order line's dedup keys are stored as <code>order_id,line_no</code>.</p>")
    out += code(KEYS, r"^def config_fingerprint\(", caption="The fingerprint: the first 12 hex characters of a hash of the document with its keys sorted.")
    out += "<h2>Validation before any write</h2>"
    out += code(LOADER, r"^def compile_domain\(", caption="Reading and checking the whole folder. Note the single raise at the end.")
    out += ("<p>Walk through the loop. For each dataset file, the manifest and its contract are parsed. "
            "<code>validate</code> checks the required fields, that the contract version the manifest pins is "
            "the contract's version, and that the history order column is a contract column. "
            "<code>setting_problems</code> checks the settings against the dictionary. "
            "<code>engine_problems</code> checks that the chosen silver engine can handle the dataset. After the "
            "loop the platform file's own settings are checked. All errors are joined into one message, so a "
            "person fixing a folder sees every problem at once.</p>")
    out += code(LOADER, r"^def setting_problems\(", caption="An unknown key is an error. A value of the wrong type is a warning.")
    out += "<h2>Writing</h2>"
    out += code(LOADER, r"^def load\(", caption="The only place that writes. Every store call comes after compile_domain has returned.")
    out += ("<p>The order of the writes follows the foreign keys: the dictionary first, because settings point "
            "at it, then the domain, then each dataset. A dataset's quality rules are written right after the "
            "dataset. The last lines compare what the store lists for the domain with what the folder declared. "
            "A dataset found only in the store is reported and left alone.</p>")
    out += code(LOADER, r"^def load_gold\(", caption="Gold tables go into the same store as datasets, after the source datasets.")
    out += "<h2>A worked example: one mistake, nothing written</h2>"
    out += ("<p>The table shows what happens when one file of the tyre folder is broken. The left column is the "
            "change, the middle column is the message, the right column is the state of the store afterwards.</p>")
    out += table(["Change to the folder", "Message", "Store afterwards"], [
        ["<code>retry_cuont: 3</code> added to the delivery manifest", "<code>setting 'retry_cuont' is not registered</code>", "Empty. No domain, no dataset, no dictionary."],
        ["The press manifest pins contract version <code>9.9.9</code>", "<code>manifest pins contract 9.9.9</code> but the contract is another version", "Empty."],
        ["<code>owners</code> removed from the inspection manifest", "<code>missing field: owners</code>", "Empty."],
        ["The press manifest file deleted after an earlier good load", "No error. The report lists <code>mes.press</code> as not in the manifest.", "The press dataset is still there."],
    ])
    out += "<h2>The settings the loader reads</h2>"
    out += table(["Manifest field", "What the loader does with it", "Required"], [
        ["<code>source_name</code>, <code>dataset_name</code>, <code>is_enabled</code>", "Identity and the enabled flag.", "Yes"],
        ["<code>config</code>", "Each key becomes a dataset setting. Each key must be in the dictionary at dataset scope.", "Yes"],
        ["<code>dedup</code>", "<code>mode</code>, <code>business_keys</code> and <code>order_by</code> become settings. The business keys also become the dedup key.", "No"],
        ["<code>contract</code>, <code>contract_version</code>", "The path to the contract and the version it must have.", "Yes"],
        ["<code>ingest.mode</code>, <code>ingest.read</code>, <code>history.kind</code>, <code>load.pattern</code>", "Copied into typed columns of the dataset row.", "<code>load</code> is required"],
        ["<code>archetypes</code>, <code>reconciliation</code>, <code>owners</code>", "Required to be present. Kept inside the manifest JSON.", "Yes"],
        ["<code>settings</code> in <code>platform.yaml</code>", "Each key becomes a domain setting. Each key must be in the dictionary at dataset or platform scope.", "No"],
    ])
    out += "<h2>How it is tested</h2>"
    out += (f"<p>{src(T_LOADER)} loads the real tyre folder into the in-memory store. It checks the nine datasets "
            "one by one, checks that loading twice gives the same fingerprints, and breaks one file at a time to "
            "prove that a bad folder leaves the store empty. A last test loads the folder into PostgreSQL as "
            "well and compares every dataset view between the two adapters.</p>")
    out += code(T_LOADER, r'        \("unregistered config key"', r"platform.yaml: setting 'timezone' is not registered",
                "The broken-folder cases: which file, what is changed, and the message that must appear.")
    out += code(T_LOADER, r"^def test_bad_manifest_refused_before_any_write", caption="The test body. The last line is the point: the store is still empty.")
    out += not_built([
        "A wrong value type is only a warning. The value is written as it is.",
        "A dataset removed from the folder stays in the store and stays enabled. The loader reports it and a "
        "person has to act.",
        "The write phase is several function calls, each its own transaction. If the connection failed halfway, "
        "some datasets would be on the new configuration and some on the old. Running the loader again repairs "
        "it, because each write replaces a dataset whole.",
        "<code>load_gold</code> checks less than the dataset path. It checks the kind and the contract version. "
        "Gold tables are stored with no settings, because no gold setting is in the dictionary yet.",
        "There is no source-level manifest file, so nothing can declare a source's own settings.",
        "The loader needs a YAML library, which is installed only for local runs and the build. The platform "
        "itself never parses YAML.",
    ])
    return out


# --------------------------------------------------------------------------------------------------------- runtime
def runtime_shape():
    d = box(10, 110, 130, 70, "run", "one per run id", "bx")
    d += box(200, 110, 170, 70, "dataset_run", "one per dataset", "solid")
    d += box(440, 10, 150, 56, "step_log", "what each step did", "bx")
    d += box(440, 82, 150, 56, "file_log", "every batch", "bx")
    d += box(440, 154, 150, 56, "error_log", "what went wrong", "bx")
    d += box(440, 226, 165, 56, "config_snapshot", "settings used", "bxs")
    d += box(670, 60, 150, 60, "watermark", "one value", "bx")
    d += box(670, 150, 150, 60, "bookmark", "named values", "bx")
    d += box(860, 105, 120, 60, "table_lease", "who writes", "bxm")
    d += arrow(140, 145, 198, 145)
    d += arrow(370, 130, 438, 40) + arrow(370, 140, 438, 110) + arrow(370, 150, 438, 180) + arrow(370, 160, 438, 252)
    d += arrow(285, 110, 668, 80, "on success only", lx=540, ly=72)
    return d


@page("meta-runtime", "The runtime tables: what happened, and the rules that keep the record honest", parent="meta",
      short="Runtime tables",
      blurb="Runs, dataset runs, steps, files, errors, watermarks, bookmarks, leases. A position moves only when its dataset run succeeds.")
def meta_runtime():
    out = ("<p class='lead'>The <code>runtime</code> schema is the platform's memory. It records every run, what "
           "each run did to each dataset, and how far each dataset has been read.</p>"
           "<p>Two kinds of trouble come from getting this wrong. If a position moves forward for a run that "
           "failed, data is skipped and nobody notices. If the record of a run can be written in any order, it "
           "cannot be trusted as evidence. The functions over these tables are built to rule out both.</p>")
    out += fig(990, 300, runtime_shape(), "The main runtime tables",
               "A run has dataset runs. The highlighted box is the unit everything hangs on. The watermark and bookmark "
               "tables are written only when a dataset run closes as succeeded.")
    out += "<h2>The tables and what one row means</h2>"
    out += table(["Table", "One row means", "Key"], [
        ["<code>runtime.run</code>", "One run of a pipeline over a domain: its id, domain, environment, what triggered it, its type and its status.", "<code>run_id</code>"],
        ["<code>runtime.dataset_run</code>", "One dataset inside one run: status, the configuration fingerprint, the watermark before and after, counts, a message, start and end times.", "<code>(run_id, dataset_id)</code>"],
        ["<code>runtime.step_log</code>", "One step of a dataset run: the step's name, a status, counts and a message.", "<code>step_id</code>"],
        ["<code>runtime.file_log</code>", "One batch that ingestion handled: batch key, file name, delivery window, checksum, declared rows, rows, status, reason and load type.", "<code>file_id</code>"],
        ["<code>runtime.error_log</code>", "One error, with the step and the error's class. The dataset may be empty.", "<code>error_id</code>"],
        ["<code>runtime.config_snapshot</code>", "One configuration document, stored once per fingerprint, with the time it was first seen.", "<code>fingerprint</code>"],
        ["<code>runtime.watermark</code>", "One dataset's watermark and the run that last moved it. A watermark is the highest change value already taken from the source.", "<code>dataset_id</code>"],
        ["<code>runtime.bookmark</code>", "One named position of a dataset, such as a page cursor, a stream position per shard, or how far silver has read the file log.", "<code>(dataset_id, bookmark_key)</code>"],
        ["<code>runtime.watermark_audit</code>", "One reset of a watermark by hand: old value, new value, reason, who, when.", "<code>audit_id</code>"],
        ["<code>runtime.table_lease</code>", "One table being written right now: the run holding it and when the lease expires.", "<code>table_name</code>"],
        ["<code>runtime.batch_void</code>", "One loaded batch that was declared bad afterwards, with the reason and the approver.", "<code>(dataset_id, batch_key)</code>"],
        ["<code>runtime.backfill_plan</code>", "One plan that names the windows gold must rebuild and the date from which its corrections count.", "<code>plan_id</code>"],
        ["<code>runtime.correction</code>", "One corrected value: the business key, the field, the new value, the old value and the dates it applies to.", "<code>correction_id</code>"],
        ["<code>runtime.dq_result</code>", "What one quality rule found in one dataset run: rows checked, rows failed, the action.", "<code>result_id</code>"],
        ["<code>runtime.dq_profile</code>", "The profile of one dataset run that loaded something.", "<code>profile_id</code>"],
    ])
    out += code(SQL, r"^CREATE TABLE IF NOT EXISTS runtime\.dataset_run", caption="The dataset run. The checks on the status column are part of the rules.")
    out += code(SQL, r"^CREATE TABLE IF NOT EXISTS runtime\.file_log", r"CHECK \(load_type IN",
                "The file log, with the index that makes the latest status of a batch quick to find, and the load type added later in place.")
    out += "<h2>A worked example: one run of the sales order lines</h2>"
    out += ("<p>The tyre company's ingestion runs for 4 November 2026 and one change file for the sales order "
            "lines has arrived with 250 rows. The values are small made-up numbers to show the shape. The left "
            "column is the table, the right column is the row the run leaves behind.</p>")
    out += table(["Table", "Row"], [
        ["<code>runtime.run</code>", "<code>ingest-2026-11-04</code>, domain tyre, environment dev, trigger schedule, type normal, status succeeded."],
        ["<code>runtime.config_snapshot</code>", "The dataset view of the sales order lines, stored under its fingerprint if that fingerprint was not seen before."],
        ["<code>runtime.dataset_run</code>", "Status succeeded, watermark before <code>4100</code>, watermark after <code>4350</code>, counts with one loaded batch and 250 rows."],
        ["<code>runtime.file_log</code>", "The file's name, window <code>2026-11-04</code>, declared rows 250, rows 250, status <code>loaded</code>, load type <code>incremental</code>."],
        ["<code>runtime.step_log</code>", "Step <code>ingest.push.change_feed</code> with status succeeded and the counts."],
        ["<code>runtime.watermark</code>", "<code>4350</code>, the highest <code>change_seq</code> loaded."],
        ["<code>runtime.bookmark</code>", "<code>feed_pos</code> with the highest feed position loaded."],
    ])
    out += "<h2>Rule 1: a position moves only when the dataset run succeeds</h2>"
    out += code(SQL, r"^-- Close a dataset run\.", END, "The one function that moves a watermark or a bookmark.", cap=40)
    out += ("<p>Walk through it. The status must be one of three words. The helper checks that the dataset was "
            "started in this run. The row is then locked with <code>FOR UPDATE</code> and must still be "
            "running, so a dataset run closes once. Only inside the branch for <code>succeeded</code> are the "
            "watermark and the bookmarks written. A watermark of nothing leaves the stored one alone. After the "
            "branch, the function reads the watermark back and writes it as <code>watermark_after</code> on the "
            "dataset run. Because the whole function is one transaction, the run record and the positions "
            "cannot disagree.</p>")
    out += ("<p>The caller cannot get around this. A pipeline has no right on <code>runtime.watermark</code> or "
            "<code>runtime.bookmark</code>, and no other function writes them apart from the audited reset.</p>")
    out += "<h2>Rule 2: a run is recorded in order</h2>"
    out += code(SQL, r"^CREATE OR REPLACE FUNCTION runtime\.start_dataset_run", END,
                "Opening a dataset run: the run must still be open, the dataset is started once, the snapshot is saved, and the current watermark is returned.")
    out += ("<p>The same pattern runs through every function. A step, a file or a profile can only be logged "
            "for a dataset that was started in that run. A run cannot be closed while a dataset in it is still "
            "running. A second start of the same run id is refused. Each of these is refusal code "
            "<code>DPM04</code>.</p>")
    out += "<h2>Rule 3: the configuration used is kept</h2>"
    out += ("<p>When a dataset run opens, the caller hands over the dataset's configuration document and its "
            "fingerprint. The document is stored in <code>runtime.config_snapshot</code> once per fingerprint, "
            "and the dataset run keeps the fingerprint. Months later, the question of which settings a run used "
            "is answered by joining the two.</p>")
    out += "<h2>Rule 4: secrets do not reach a message</h2>"
    out += code(SQL, r"^-- Secrets out, then capped", r"^\$\$;", "Every stored message passes through this function first.")
    out += ("<p>The inner replacement finds a name such as password, token or secret followed by an equals sign "
            "and replaces the value up to the next separator with three stars. The outer one does the same for a "
            "bearer token. The result is cut to 4000 characters. Step messages, file reasons, error messages, "
            "dataset run messages and quality result details all pass through it. The same two patterns are in "
            f"{src(PORT)} for the in-memory adapter.</p>")
    out += "<h2>Rule 5: one writer per table, with a time limit</h2>"
    out += code(SQL, r"^-- NULL when this run holds the lease now", END, "Taking a lease is one insert that turns into an update only when the old lease has expired or belongs to the same run.")
    out += ("<p>A lease is a row that says a run is writing a table. The insert succeeds when no row exists. "
            "When a row exists, the update happens only if the row's expiry time has passed or the row already "
            "belongs to this run. The function then reads the row and answers with nothing if this run holds "
            "it, or with the other run's id. A run that crashes leaves its lease behind, and the lease frees "
            "itself when its time is up.</p>")
    out += code(LEASES, r"^LEASE_SECONDS = ", 1, "The time limit every runner passes: four hours.")
    out += code(LEASES, r"^def busy\(", caption="What a runner does when the table is busy: a failed step that names the holder, and a dataset run closed as skipped.")
    out += "<h2>Rule 6: the file log is never edited</h2>"
    out += ("<p>The file log only grows. A batch that is handled twice has two rows, and the newest one gives "
            "its status. A loaded batch that later turns out to be bad is not deleted. It gets a row in "
            "<code>runtime.batch_void</code>, with who approved it and why, and from then on every read reports "
            f"its status as <code>void</code>. {link('silver-reader')} shows how silver uses that.</p>")
    out += "<h2>Rule 7: a reset by hand leaves a trace</h2>"
    out += code(SQL, r"^CREATE OR REPLACE FUNCTION runtime\.reset_watermark", END, "The only other way a watermark changes. The audit row is written first.")
    out += "<h2>The settings this part reads</h2>"
    out += table(["Value", "Where it is", "Meaning"], [
        ["4 hours", "<code>LEASE_SECONDS</code> in the leases module", "How long a lease lasts before another run may take it over."],
        ["4000 characters", "<code>runtime._redact</code> and <code>ERROR_CAP</code> in the port", "The longest message kept."],
        ["10 characters", "The SQL functions and <code>MIN_REASON</code> in the port", "The shortest reason accepted for a reset, a void or a backfill plan."],
        ["normal, reload, backfill", "The check on <code>runtime.run.run_type</code>", "The three run types. Only a normal ingestion run moves positions."],
    ])
    out += "<p>None of these is a setting in the dictionary. They are constants in the code.</p>"
    out += "<h2>How it is tested</h2>"
    out += code(T_STORE, r'    \("watermark: advances on success"', 8, "Three real cases: the watermark moves on success, stays on failure, and stays when a success passes none.")
    out += code(T_STORE, r'    \("lease: a second run finds it busy"', 2, "The lease case: the second run is told who holds the table.")
    out += (f"<p>The same table in {src(T_STORE)} has a case that logs an error message containing a password, a "
            "token and a bearer header, and checks that the stored message shows three stars in their place. "
            "Another case logs a message of 9000 characters and checks that 4000 are kept.</p>")
    out += not_built([
        "The configuration snapshot is keyed by the dataset's fingerprint, and that fingerprint covers the "
        "dataset manifest and the contract. It does not cover a registry default or a domain setting. If one "
        "of those changed while the dataset's own files did not, the fingerprint would be the same and the "
        "snapshot already stored would be kept. The record would then show the older merged settings.",
        "A lease has a fixed length and is not renewed while a run is working. A write that took longer than "
        "four hours could be overtaken by a second run.",
        "A run that crashes leaves its run and dataset run rows with status running. I found no function that "
        "closes them afterwards. A run id can be started only once, so the rerun needs a new run id.",
        "Redaction is by pattern. A secret that does not look like a name, an equals sign and a value, or like "
        "a bearer token, would pass. Counts, file names and the configuration snapshot are not redacted.",
        "There is no function to reset a bookmark, only a watermark. And the audited watermark reset has no "
        "caller in the platform code yet.",
        "<code>runtime.dq_result</code> has no writer in the platform code that I could find. Silver records "
        "what its quality stage found in the step log instead.",
        "Nothing removes old rows from the runtime tables. They grow with every run.",
        "The run id is planned to become the trace id. The comment on the table says so. Today it is a text the "
        "runner builds.",
    ])
    return out
