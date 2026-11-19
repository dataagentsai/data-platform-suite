"""The semantic layer: metrics defined once, checked against known answers, and what is generated from them."""
from lib import *  # noqa: F401,F403

METRICS = "P/src/dataplatform/semantic/metrics.py"
ANSWERS = "P/src/dataplatform/semantic/answers.py"
EXPER = "P/src/dataplatform/semantic/experiences.py"
GOV_TABLES = "P/src/dataplatform/governance/tables.py"
GOV_CATALOG = "P/src/dataplatform/governance/catalog.py"
ACCESS = "P/src/dataplatform/access/model.py"
REALM = "P/realms/databricks/semantic_governance/"
GEN = REALM + "generated/tyre/dev/"
ENGINE_TASK = "P/realms/databricks/engine/tasks/semantic_layer.py"
MV = "C/slices/02-tyre/manifest/metrics/"
T_METRICS = "P/tests/dataplatform/test_semantic_metrics.py"
T_EXPER = "P/tests/dataplatform/test_semantic_experiences.py"
T_REALM = "P/tests/databricks_realm/test_semantic_governance.py"
T_ACCESS = "P/tests/dataplatform/test_access_model.py"
T_CATALOG = "P/tests/dataplatform/test_governance_catalog.py"


def semantic_shape():
    d = box(10, 150, 150, 80, "Metric file", ["one per", "metric view"], "bx")
    d += box(215, 150, 160, 80, "Read and lint", ["refuses a", "bad definition"], "solid")
    d += box(430, 60, 170, 80, "Certified view", ["certified", "measures only"], "bx")
    d += box(430, 240, 170, 80, "Preview view", ["draft and reviewed", "measures too"], "bxm")
    d += box(660, 20, 150, 70, "Known answers", ["asked of gold"], "bx")
    d += box(660, 110, 150, 70, "Dashboards", ["one per domain"], "bx")
    d += box(660, 200, 150, 70, "Question space", ["certified only"], "bx")
    d += box(850, 110, 130, 70, "Gold", ["facts and", "dimensions"], "bxs", href="gold.html")
    d += arrow(160, 190, 213, 190)
    d += arrow(375, 175, 428, 110) + arrow(375, 205, 428, 270)
    d += arrow(600, 85, 658, 55) + arrow(600, 100, 658, 140) + arrow(600, 115, 658, 225)
    d += arrow(810, 55, 848, 125) + arrow(810, 145, 848, 145)
    d += text(495, 350, "Everything to the right of the highlighted box is generated. Nothing there is written by hand.", "tx", "middle")
    return d


@page("semantic", "The semantic layer: a metric defined once and checked against a known answer", parent="index",
      short="Semantic layer",
      blurb="Metric files become views, dashboards and a question space. Each certified metric must reproduce a number the simulated company knows.")
def semantic():
    out = ("<p class='lead'>Gold holds the rows. The semantic layer holds the meaning: what exactly is the "
           "on-time-in-full percentage, which columns it is computed from, and by which dimensions it may be "
           "cut. A metric is one such named calculation. It is defined once, in a metric file, and everything "
           "that shows it is generated from that file.</p>"
           "<p>Without this, the same word ends up with two formulas. One dashboard counts cancelled orders and "
           "another does not, and nobody can say which number is right. Here a metric cannot be certified "
           "until it comes with a known answer: a number, at a stated place and time, that the simulated tyre "
           "company knows to be true.</p>")
    out += fig(990, 370, semantic_shape(), "From a metric file to everything that shows the metric",
               "Read it left to right. The highlighted box is the gate. The dashed box holds measures that are not certified yet, and nothing on the right reads it.")
    out += "<h2>The steps</h2>"
    out += ladder([
        ("Read the metric file", "Each file names one gold fact as its source, the dimensions it joins, the dimensions a reader may cut by, and the measures. A source that is not a gold fact, or a join the fact does not have, is refused."),
        ("Lint", "Linting is checking a definition against rules before using it. The rules cover the view's name, the lifecycle state of each measure, a known answer on every certified measure, and one meaning per word. Any problem stops generation."),
        ("Split by certification", "Certified measures go into the view that carries the file's name. Measures that are still draft or reviewed go into a second view with the suffix <code>_preview</code>. Deprecated measures go nowhere."),
        ("Ask the known answers", "Each certified measure's known answer is turned into a query and asked of gold. The result must equal the oracle of the same run."),
        ("Generate what people use", "One dashboard per domain and one question space are derived from the certified views only."),
        ("Render for the product", "A separate folder turns all of the above into files for Databricks: view definitions, the known-answer query, dashboard files, the question space, catalog tags, grants and masks."),
    ])
    out += ("<p>The first five steps live in <code>src/dataplatform/semantic</code> and name no product. Only "
            "the last step knows about Databricks. This is the same split as everywhere else in the platform: "
            "a kernel that holds the rules, and a realm that renders them for one product.</p>")

    out += "<h2>A metric file</h2>"
    out += code(MV + "mv_otif.yaml", r"^kind: metric_view", r"value: 107188\}",
                "The metric file for on time in full: one source fact, two joins, three dimensions, two measures.")
    out += ("<p>Read the first measure. Its expression counts lines that were on time and in full, over the "
            "rows whose window is complete, and divides by the number of such rows. It is not additive, which "
            "means it cannot be summed across months. Its time dimension is the promised month. Its synonyms "
            "are the other words people use for it. Its known answer says that for the south region in "
            "November 2026 the value is 0.8961, give or take 0.0005.</p>")
    out += "<h2>The keys of a metric file</h2>"
    out += "<p>The table lists each key, what it means, and its allowed values and default, as <code>metric_view</code> and <code>lint</code> read them.</p>"
    out += table(["Key", "What it means", "Allowed values and default"], [
        ["<code>kind</code>", "Marks the file as a metric view.", "<code>metric_view</code>. Anything else is refused."],
        ["<code>name</code>", "The view's name.", "Must start with <code>mv_</code> and be lower snake case."],
        ["<code>domain</code>", "The domain that owns the view. It decides the gold schema.", "Required."],
        ["<code>source</code>", "The gold fact the measures are computed over.", "Must be a gold manifest whose name starts with <code>fact_</code>."],
        ["<code>joins</code>", "The gold dimensions joined to the fact.", "Each must be a dimension of the source fact. The join key comes from the fact's manifest, not from this file."],
        ["<code>dimensions[].name</code>, <code>.expr</code>", "A name a reader cuts by, and the expression behind it.", "An expression that reads <code>dim_x.column</code> is marked as coming from that dimension."],
        ["<code>measures[].name</code>, <code>.expr</code>", "The metric's name and its formula.", "A name may be defined in one view only."],
        ["<code>measures[].additive</code>", "Whether the measure may be summed across its dimensions.", "True or false. Default false."],
        ["<code>measures[].time_dimension</code>", "The dimension that is the measure's time axis.", "Must be one of the view's dimensions."],
        ["<code>measures[].certification</code>", "Where the measure is in its life.", "<code>draft</code>, <code>reviewed</code>, <code>certified</code> or <code>deprecated</code>. Default <code>draft</code>."],
        ["<code>measures[].synonyms</code>", "Other words for the measure.", "Among certified measures, one word may mean one measure only."],
        ["<code>measures[].known_answer.where</code>", "The place and time of the known answer.", "Dimension equals value, joined by AND. A value is quoted text, a month as <code>2026-11</code>, or a day as <code>2026-11-12</code>."],
        ["<code>measures[].known_answer.value</code>, <code>.tolerance</code>", "The number, and how far a result may be from it.", "Tolerance defaults to 0. Required on a certified measure."],
        ["<code>owners.dataset</code>", "The team that owns the view.", "Required."],
    ])

    out += "<h2>Reading and linting</h2>"
    out += code(METRICS, r"^def metric_view\(", caption="One metric file becomes one record. The join keys are looked up in the gold manifests.")
    out += ("<p>The function takes the file and the gold manifests. It looks the source fact up and refuses a "
            "source that gold does not have. For each join it takes the key column from the fact's own "
            "manifest, so the metric file only names a dimension and can never state a wrong key. It then "
            "builds the dimensions and the measures.</p>")
    out += code(METRICS, r"^def lint\(", caption="Every rule a set of metric views must pass before anything is generated.", cap=40)
    out += ("<p>The rules are in order: the view's name, the source's name, each join's name, an owner, then "
            "for each measure a valid lifecycle state, a time dimension the view has, and a known answer if it "
            "is certified. The last two rules look across all views. A measure name that appears in two views "
            "is an error, because a metric is defined once. Among certified measures, a name or synonym that "
            "already means another measure is an error, because one word must mean one metric.</p>")
    out += "<h2>Certified view and preview</h2>"
    out += code(METRICS, r"^def published\(", caption="Which views are created from one metric file.")
    out += ("<p>The table shows what this does to the three tyre metric files. Each row is one file. The "
            "middle column is the view that dashboards and the question space read. The right column is the "
            "preview, which holds every measure that is not deprecated.</p>")
    out += table(["Metric file", "Certified view and its measures", "Preview view and its measures"], [
        ["<code>mv_otif</code>", "<code>mv_otif</code>: OTIF %, Order lines", "none, because every measure is certified"],
        ["<code>mv_sales</code>", "<code>mv_sales</code>: Sell-in units, Sell-out units", "<code>mv_sales_preview</code>: the two above and Net sales value, which is a draft"],
        ["<code>mv_scrap</code>", "<code>mv_scrap</code>: Scrap rate %", "<code>mv_scrap_preview</code>: Scrap rate % and Tyres cured, which is reviewed"],
    ])

    out += "<h2>Known answers: one number everywhere</h2>"
    out += ("<p>A known answer is a small test that travels with the metric. It says where to look and what "
            "number must be there. The same known answer is used three ways: it is looked up in the oracle, "
            "which is the simulated company's own record of what is true, it is asked of gold, and it is asked "
            "of the view on the engine. All three must agree.</p>")
    out += code(METRICS, r"^def parse_where\(", caption="The text of a known answer's place and time becomes a list of filters on the view's dimensions.")
    out += code(METRICS, r"^class KnownAnswerCase:", r"return \"/\"\.join\(time \+ rest\)",
                "One known answer as a test case, and the address under which the oracle keeps the same number.")
    out += "<h3>A worked example</h3>"
    out += ("<p>The known answer of OTIF % reads <code>region = 'south' AND promised month = 2026-11</code>. "
            "Each row below is one form the same question takes.</p>")
    out += table(["Where it is asked", "What the question looks like"], [
        ["As filters", "Region equals south. Promised month is the month 2026-11."],
        ["In the oracle", "The measure <code>OTIF %</code> at the address <code>2026-11/south</code>. The time value comes first, then the other values."],
        ["Of gold", "The measure's expression over <code>fact_otif</code> joined to <code>dim_dealer</code> and <code>dim_material</code>, filtered on the two dimension expressions, in the business time zone."],
        ["Of the view on the engine", "<code>MEASURE(`OTIF %`)</code> from <code>mv_otif</code> with the same two filters."],
    ])
    out += code(ANSWERS, r"^def question_sql\(", caption="The question written the way the metric view defines it, to be asked of gold directly.")
    out += ("<p>This is what lets a laptop check the metric before any workspace exists. The query is built "
            "from the metric file alone: the measure's expression, a left join to each dimension on its key, "
            "and one condition per filter. It runs through the gold reader, on Spark or on DuckDB, with the "
            "session set to the business time zone so that a day means a business day.</p>")
    out += code(METRICS, r"^def check_known_answers\(", caption="Each case is asked of whatever can answer it. A missing value fails.")
    out += "<h3>What the recorded runs say</h3>"
    out += ("<p>Each row is one known answer. The second column is the value in the metric file. The third is "
            "what gold gave in the recorded 14-day run of 5 November 2026 at one tenth of full scale, which "
            "equalled the oracle of that run in every row. The fourth is what gold gave on 4 November 2026 when "
            "it was built once over the full-scale silver of day 14.</p>")
    out += table(["Known answer", "In the metric file", "Gold at one tenth scale", "Gold at full scale, built once"], [
        ["OTIF %, south, November", "0.8961", "0.8851", "0.8961"],
        ["Order lines, November", "107,188", "10,216", "107,188"],
        ["Sell-in units, north, November", "1,279,642", "115,637", "1,279,642"],
        ["Sell-out units, north, October", "150,803", "150,803", "150,803"],
        ["Scrap rate %, plant P-CHN, 12 November", "0.0180", "0.017981", "0.017981"],
    ])
    out += (f"<p>The values in the metric files are true at full scale after 14 days. At one tenth scale three "
            f"of the five depend on the number of orders and so differ from the file, while still equalling "
            f"the oracle of their own run. Dealer files and presses do not scale, so the last two match at any "
            f"scale. In that run the known-answer tests gave 12 passed and 4 failed, and the 4 were exactly the "
            f"scale-dependent ones. A separate test checks that the full-scale oracle equals every value in the "
            f"metric files, and its note says it passed on 4 November 2026. The same five numbers were "
            f"reproduced on DuckDB. The sources are {src('P/docs/W1D-GOLD.md')} and {src('P/docs/DUCKDB-SPIKE.md')}.</p>")
    out += note("A known answer found a wrong definition",
                "<p>The first version of OTIF % divided by every order line of the month. Asked of gold it gave "
                "0.6706 at full scale where the oracle said 0.8961, because lines whose promised date had not "
                "come yet were counted as missed. The definition was changed to count only rows whose window is "
                "complete, which is the filter you see in the metric file above. This is the kind of mistake "
                "the known answer exists to catch.</p>")

    out += "<h2>Dashboards and the question space</h2>"
    out += ("<p>Nothing that people look at is drawn by hand. A dashboard is derived per domain: one page per "
            "certified view, and for each certified measure a headline number, a trend over its time dimension, "
            "and a breakdown by each other dimension. A question space is a place where a person asks in plain "
            "words and a model answers from the data. The platform generates its definition too.</p>")
    out += code(EXPER, r"^def conversation_space\(", caption="The question space: certified views only, instructions, sample questions, and the measures it must decline.", cap=32)
    out += ("<p>Three things make this safe. Only certified views are attached, so the space cannot read a "
            "draft measure even if asked. The instructions list each business word beside the measure it "
            "means. And the measures that exist but are not certified are listed by name, with the instruction "
            "to say so and not to estimate. For the tyre company those are Net sales value and Tyres cured.</p>")

    out += "<h2>What is generated for Databricks</h2>"
    out += (f"<p>The folder {src(REALM + 'generate.py')} reads the tyre manifests, contracts and an access file, "
            "and writes one set of files per environment. The files are committed. A test fails if a committed "
            "file differs from what the generator would write now, so a hand edit cannot survive.</p>")
    out += table(["Generated file", "What it is", "Made from"], [
        ["<code>semantic/mv_*.sql</code>", "One statement per published view that creates a metric view.", "The metric files and the gold manifests"],
        ["<code>semantic/known_answers.sql</code>", "One query that asks every known answer of the views and says whether each is within tolerance.", "The known answers"],
        ["<code>bi/*.lvdash.json</code>", "One dashboard file per domain.", "The certified views"],
        ["<code>bi/genie_space.json</code>", "The question space.", "The certified views, synonyms and known answers"],
        ["<code>governance/catalog_sync.sql</code>", "Comments and tags for every table, view and column.", "The contracts, the manifests and the metric views"],
        ["<code>governance/audit.sql</code>", "A query over the audit log for reads of tables with protected columns and changes to grants and tags.", "The contracts"],
        ["<code>access/access.sql</code>", "Grants to groups and a mask function per protected column.", "The access file and the contracts' classifications"],
        ["<code>findings.md</code>", "Differences between a contract and its manifest that were reported instead of being quietly resolved.", "Contracts and manifests"],
    ])
    out += code(GEN + "semantic/mv_otif.sql", r"^CREATE OR REPLACE VIEW", r"^\$\$;",
                "The generated metric view for on time in full, for the dev environment.", cap=40)
    out += ("<p>Compare it with the metric file. The source is now the full three-part name of the gold fact. "
            "Each join has the key from the fact's manifest and is declared many to one, which is true because "
            "a dimension has one row per key, so no measure can be counted twice. The expressions are copied "
            "unchanged. The comments are composed from the description, the certification and the time "
            "dimension.</p>")
    out += code(REALM + "render.py", r"^def metric_view_sql\(", caption="The renderer: a record in, text out, the same text every time for the same input.")
    out += code(GEN + "semantic/known_answers.sql", r"^SELECT \*, got IS NOT NULL", 5,
                "The start of the generated known-answer query: expected, tolerance, and what the view gives.")
    out += code(ENGINE_TASK, r"^def check\(", caption="After a run on the engine: each view's number beside gold's and the world's.")
    out += ("<p>This last function is the three-way comparison. It runs the generated query against the views "
            "that were just created, and for each row takes the number gold gave in Python and the number the "
            "simulated world holds. A row is in order only if the view equals both. The value from the metric "
            "file is carried along and reported, not judged, because it only holds at full scale.</p>")

    out += "<h2>Governance: what the catalog must say</h2>"
    out += ("<p>A data catalog is the place where tables are listed with their owners, descriptions and "
            "labels. The platform does not ask anyone to fill it in. It works out what the catalog must say "
            "from the contracts and manifests, compares that with what the catalog holds, and produces the "
            "smallest list of changes.</p>")
    out += code(GOV_TABLES, r"^TABLE_TAGS = ", 4, "The tag standard: the only tag keys the platform ever sets or removes.")
    out += code(GOV_CATALOG, r"^def _diff\(", r"^    return actions", "The difference for one table or column.")
    out += code(GOV_CATALOG, r"^def plan_sync\(", caption="The plan for the whole catalog, and also the drift report: an empty plan means the catalog is in step.")
    out += table(["Situation", "What the plan does"], [
        ["The catalog is empty", "Writes every comment and every tag."],
        ["Someone changed an owner tag by hand", "Sets it back."],
        ["Someone added a tag of their own", "Leaves it alone. Only keys in the tag standard are ever touched."],
        ["A tag of ours is no longer wanted", "Removes it."],
        ["A table is in the catalog but no contract declares it", "Removes only our tags from it."],
        ["A table in bronze", "Never touched. Only silver and gold have contracts."],
        ["A contract has no description", "The existing comment is not cleared."],
    ])
    out += ("<p>Each column gets a classification tag. A contract that says nothing means "
            "<code>internal</code>. A column classified <code>restricted</code> is personal data that was "
            "replaced by a token before it reached bronze, and is tagged as tokenised. A column classified "
            "<code>confidential</code> is tagged as masked. A data product is a gold fact with its metric "
            "views, and each silver table and dimension is tagged with the products that read it.</p>")

    out += "<h2>Access: groups, masks and row filters</h2>"
    out += ("<p>Access is given to groups, never to a named person. An access file lists the groups. For each "
            "it says which layers the group reads, which domains, and whether it sees protected columns or "
            "sees them masked.</p>")
    out += code(REALM + "access/tyre.yaml", r"^groups:", r"^row_filters:", "The access file for the tyre company.", cap=30)
    out += code(ACCESS, r"^def plan_access\(", caption="From groups and classified columns to grants, masks and row filters.", cap=36)
    out += ladder([
        ("Grants", "A group that reads every domain gets read on the whole layer. A group limited to some domains gets read on each of its tables, and permission to enter the layer and the schema."),
        ("Masks", "Every column classified confidential or restricted gets a mask. The groups allowed past the mask are those declared <code>see</code> that can read the table."),
        ("Row filters", "A filter names a table, a column, the groups that see every row, and the values each limited group may see. A reader not named on the filter sees no rows. A filter on a table that is not published is refused."),
    ])
    out += code(ACCESS, r"^def visible_columns\(", caption="The check: what one group sees in each column of one table.")
    out += ("<p>This function is how the plan is tested without any database. For a group and a table it "
            "returns, per column, one of three words. <code>clear</code> is the real value. "
            "<code>token</code> is the stored token of a restricted column, which is all even an allowed group "
            "ever sees, because nothing in this code turns a token back into the value. <code>masked</code> is "
            "the mask. If the group cannot read the table at all it returns nothing.</p>")
    out += "<h3>A worked example</h3>"
    out += ("<p>The silver dealer table has a dealer code that is internal and a contact e-mail column that "
            "is restricted. Each row is one group from the access file and what it sees.</p>")
    out += table(["Group", "Can read silver dealers?", "dealer code", "contact e-mail"], [
        ["<code>sales-data</code>", "yes, its own domain", "clear", "the token"],
        ["<code>platform-engineers</code>", "yes, every domain", "clear", "masked"],
        ["<code>data-platform-jobs</code>", "yes, every domain", "clear", "the token"],
        ["<code>manufacturing-data</code>", "no, another domain", "", ""],
        ["<code>analysts</code>", "no, gold only", "", ""],
    ])
    out += code(GEN + "access/access.sql", r"^CREATE OR REPLACE FUNCTION `dev_ops`\.`access`\.`mask_silver_dealer_api_dealer_contact_email`", 5,
                "The generated mask for that column: a function that returns the stored value to two groups and three stars to everyone else.")

    out += "<h2>What was run where</h2>"
    out += ("<p>This matters, so it is stated plainly. Each row is one part of this page. The second column "
            "says what the public code and its recorded notes show was actually run.</p>")
    out += table(["Part", "What the evidence shows"], [
        ["Reading, linting, splitting, known-answer parsing", "Tested locally with no engine, as tables of cases."],
        ["Known answers asked of gold", "Run locally on Spark and on DuckDB over a 14-day simulated run at one tenth of full scale, recorded on 5 November 2026. Gold built once over full-scale silver on 4 November 2026 gave the metric files' values. The full-scale 14-day run is owed."],
        ["Generated view, dashboard and question space files", "Generated and committed. Tested locally for content and for being up to date. These tests read the files. They do not run them."],
        ["Creating the metric views on a workspace and checking them against gold and the world",
         "The code that does this after a run on the engine is in the repository. The public snapshot holds no saved result of such a run, so this page cannot say what the views returned on a workspace."],
        ["The question space on a workspace", "A deploy script exists. Comments in the code record a first deploy on 15 November that found the product refuses lists that are not sorted, which the renderer now sorts. No transcript of questions and answers is in the snapshot."],
        ["Dashboards on a workspace", "A bundle file that deploys the two dashboards exists. Whether it was deployed cannot be told from the snapshot."],
        ["Catalog sync, grants, masks and the audit query", "Generated and tested locally only. The adapter that reads a live catalog is tested against made-up rows. Nothing in the snapshot applies these statements to a workspace."],
        ["A masked column read by a user without privilege", "Not run. The design note lists it as waiting for a workspace."],
    ])

    out += "<h2>How it is tested</h2>"
    out += table(["Test file", "What its tables cover"], [
        [src(T_METRICS), "Parsing a known answer's filters, refusals when reading, every lint rule, the split into certified and preview, looking a case up in the oracle, the verdict with a tolerance, and the tyre files themselves."],
        [src(T_EXPER), "One dashboard per domain from certified views only, the tiles per measure, and what the question space attaches, declines and asks."],
        [src(T_REALM), "The committed files are what the generator writes, generation is repeatable, identifiers and values are quoted safely, and the tyre output has the properties it must have."],
        [src(T_ACCESS), "What each group sees per column, row filters, grants, masks, and refusals of a bad access file."],
        [src(T_CATALOG), "Tables from contracts, who reads what, the sync plan for each kind of drift, and that applying a plan leaves nothing to do."],
    ])
    out += code(T_METRICS, r"^LINT = \{", r"^\}", "The lint rules as a table: a set of views, and a piece of each error it must produce. An empty list means clean.")
    out += code(T_METRICS, r"^def test_metric_view_matches_world_oracle_pending_gold",
                caption="One number everywhere: for every known answer, the view's number and the oracle's number of the same run must be within the tolerance.")
    out += ("<p>The word pending in the test's name is historical. It was written before gold existed and "
            "skips unless two files from a run are named. The build command for the known answers names them.</p>")
    out += code(T_ACCESS, r"^SEES = \{", r"^\}", "What each group must see: a group, a table, and the expected word per column, or nothing when it cannot read the table.")
    out += code(T_REALM, r"^def test_genie_reads_certified_views_only", caption="The generated question space attaches the certified views and names the two measures it must decline.")
    out += code(T_CATALOG, r"^def test_plan_converges\(", caption="For every kind of drift, applying the plan and planning again gives an empty plan.")

    out += not_built([
        "No result of the generated views, dashboards or question space running on a real workspace is in the public snapshot. Treat everything in the Databricks folder as generated and locally tested unless the table above says more.",
        "The catalog sync, the grants, the masks and the audit query have not been applied to a live catalog as far as the snapshot shows.",
        "Grants are planned, not compared with what exists. The generated file grants what the model says and does not revoke a grant someone made by hand.",
        "No row filter is declared for the tyre company. The mechanism is built and tested with a made-up filter.",
        "The access file lives in the Databricks folder of the platform repository, not with the tyre manifests.",
        "The catalog has no glossary object. Glossary terms are the certified measure names, written as a tag and in the view's comment.",
        "The contract and the manifest of the inspection results name different owners. The generator reports the difference and takes the contract's owner. Nobody has decided which is right.",
        "The business day of a view's time dimension depends on the session's time zone. Locally the reader sets it. On a workspace the warehouse must be set to the business time zone, and nothing in the generated files does that.",
        "Sample questions are derived from the known answers and the first synonym. A metric file cannot yet carry its own wording for them.",
        "Net sales value and Tyres cured are not certified, so they appear on no dashboard and the question space declines them.",
        "The metric files' known answers hold only at full scale after 14 days. At any other scale the test against the file's value fails for the measures that depend on order volume, and only the test against the oracle of the same run is meaningful.",
    ])
    return out
