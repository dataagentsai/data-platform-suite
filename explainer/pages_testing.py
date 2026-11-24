"""The testing strategy: why a list of cases is not enough, the approaches, the data twin, the tiers and the open-source tools."""
from lib import *  # noqa: F401,F403

MODEL = "P/src/dataplatform/silver/history_model.py"
PROPS = "P/tests/dataplatform/test_silver_properties.py"
STAGES = "P/tests/dataplatform/test_silver_stages.py"

APPROACHES = [
    ["Table-driven cases", "A named list of inputs and the expected outputs.", "Situations someone thought of, and regressions.", "Built: 144 stage tests."],
    ["Property tests against a reference model", "Generate hundreds of random inputs. A small, obviously correct model says what the result must be, and the real code is compared with it.", "Cases nobody listed.", "Built for SCD Type 1 and Type 2."],
    ["Metamorphic tests", "The same data in another order, or split into other batches, must give the same result.", "Bugs that depend on order or batching.", "Built for SCD Type 2."],
    ["Known-bug registry", "Each real gap is a minimal case marked as an expected failure that is strict: when someone fixes the bug, the case fails and says to remove the mark.", "Bugs that are forgotten, or fixed without anyone noticing.", "Built: 4 cases."],
    ["Mutation testing", "Break the code on purpose (change a greater-than to greater-or-equal) and check that a test fails. The share caught is the strength of the tests.", "Tests that run a line but would not notice if it were wrong.", "Planned. Not run yet."],
    ["Pairwise combinations", "Test every pair of option values together in a few dozen cases, not every full combination.", "Settings that interact badly.", "Counted: 23 cases instead of 2,880. Not built."],
    ["Hostile values", "Generate awkward numbers, dates and text for type conversion and compare with a reference parser and across two engines.", "The odd forms real data arrives in.", "Planned."],
    ["Full-copy and snapshot contract", "The same idea for input that is not a change feed, with a model that compares successive copies.", "The inputs the declarative engine refuses today.", "Planned."],
    ["The data twin at scale", "Many random seeds, the history rules checked every simulated day, the same day delivered in another order.", "Surprises that only appear end to end.", "Planned."],
    ["Production corpus", "Real batches, made anonymous, replayed on every change.", "Whatever no simulation imagined.", "Needs real data."],
]

FINDINGS = [
    ["1", "A delete that arrives before the update it follows overwrites the stored version's value, and the earlier history is lost for good.", "Silent loss of history"],
    ["2", "The same delete sent twice in one batch becomes two versions of the key.", "Duplicate rows"],
    ["3", "A late change that lands inside a run of same-value changes makes the later change forgotten. (Found by review on 19 Nov, before the generated tests.)", "A lost change"],
    ["4", "A key deleted and then changed back to the same value is one live version when the two arrive in separate batches, and two versions when they arrive in one batch.", "The result depends on how the feed was split"],
]

PLUG_INS = [
    ["The reference model", "The oracle. Today it judges counts and sums. It would also judge versions, worked out from the company's own change log.", "Move the model into the platform code so tests and twin share it. Done."],
    ["Invariant checks (one current version, no overlap)", "The runner's silver checks, every simulated day.", "A few checks in the existing list."],
    ["The four found gaps", "Planted incident kinds in the world file, so each exists end to end.", "Four incident entries with their expected results."],
    ["Random seeds and shrinking", "The world's seed, and the learning loop that turns a failed run into a pending scenario file.", "A seed loop on the fast engine, and a bridge from the shrinker to the loop."],
    ["Reordered, split and repeated delivery", "The release gate's clone-and-compare: two copies, the same input delivered differently, silver must match.", "A delivery shaper."],
    ["Hostile values", "A new incident kind with parameters. The company knows exactly which rows it made unconvertible.", "The shaper and the expected quarantine count."],
    ["Pairwise variants", "Variant copies of the slice, each of which must equal the oracle.", "A generator of option combinations."],
]

TIERS = [
    ["0", "Foundations: tests run the same everywhere", "Declared test dependencies, make targets, tests on UTC, a clean static scan, the scan covering the Databricks code too", "Pinned versions, a nightly property run, one test-adequacy page", "Laptop"],
    ["1", "Known gaps: make them visible, then fix them", "The shared model, four strict known-bug cases", "Three owner decisions, then the fixes", "Laptop"],
    ["2", "Stronger tests for each step", "Property and metamorphic tests for SCD Type 1 and 2", "Mutation testing, hostile values, the other stages, the snapshot contract", "Laptop"],
    ["3", "Configuration coverage", "The count of pairwise cases", "Generate and run them; refusal tests", "Laptop, then workspace"],
    ["4", "The data twin does the same end to end", "Nothing yet", "Version-level oracle, daily invariants, incident kinds, seeds, shapers", "Laptop"],
    ["5", "Proof on the real platform", "Deployment gates, benchmark and maintenance probes (built, not run)", "The runs on a workspace", "Workspace"],
    ["6", "Production feedback", "The learning loop for planted incidents", "A real-data corpus; real failures becoming scenarios", "Needs real data"],
]

IN_USE = [
    ["pytest, pytest-cov", "Running tests and measuring coverage", "Everywhere. Coverage has a floor of 80%."],
    ["Hypothesis", "Property-based testing: generates the inputs and shrinks a failure to a minimal case", "The property tests."],
    ["PySpark with delta-spark", "The engine the stages run on", "Stage and gold tests."],
    ["SQLFrame on DuckDB", "The same DataFrame code on a fast local engine", "The same stage tests run on a second engine; the property tests run here for speed."],
    ["delta-rs, pyarrow, psycopg", "Reading and writing tables and PostgreSQL locally", "Local tables and sources."],
    ["moto", "A fake AWS for S3 and Kinesis", "The cloud adapter tests."],
    ["allpairspy", "Generates pairwise cases", "Used once to count the cases. Not yet a part of the suite."],
    ["ruff, mypy, bandit, semgrep, sqlfluff", "Style, types, security patterns and SQL lint", "Every pull request."],
    ["gitleaks, pip-audit, cyclonedx-bom", "Keys in the history, vulnerable dependencies, a software bill of materials", "Every pull request."],
    ["pyspark-antipattern, sparkdoctor, complexipy", "Spark-specific and complexity rules", "Through the project's own static-analysis bundle."],
]


@page("tests-strategy", "The testing strategy: generated tests, the data twin, the tiers and the tools", parent="tests",
      short="Testing strategy",
      blurb="Why a list of cases is not enough, what we add to it, and in what order.")
def strategy():
    out = ("<p class='lead'>A test can run every line of code and still prove nothing, because running a line does not "
           "mean the test would notice if the line were wrong. A hand-written list of cases is also only as complete as "
           "the person who wrote it. This page explains what we do about both, in the order we do it.</p>")
    out += ("<p>The guiding rule is to <b>fail fast</b>: find a wrong rule at the cheapest layer that can show it. Every "
            "bug the generated tests have found so far was found on a laptop, in seconds to minutes, before any "
            "workspace or real data was involved.</p>")
    out += "<h2>Where a test runs, from cheapest to most expensive</h2>"
    out += ladder([
        ("A pure function on a laptop", "Each silver step takes a table in and gives a table out, so it runs with no database and no cluster."),
        ("The same step on a fast second engine", "The same test tables run on DuckDB through SQLFrame, so a run takes seconds."),
        ("The same step on local Spark", "The engine the platform really runs on, on one machine."),
        ("The simulated company, end to end", f"The data twin: a generated company writes into mock sources and every layer is judged against known answers. See {link('tests-world')} and {link('tests-oracle')}."),
        ("A real workspace", "Real tables, real formats, real cost, a real catalog."),
        ("Production", "Data-quality rules, reconciliation and drift checks watching real data."),
    ])
    out += ("<p>The first three layers are where most bugs can be found cheaply. The rest are needed too, because they "
            "show what a laptop cannot: format behavior, cost, scale and data nobody imagined.</p>")
    out += "<h2>What we had, and what was missing</h2>"
    out += (f"<p>{link('tests')} describes three kinds of proof: table-driven cases, the simulated company with an oracle, "
            "and planted incidents. All three start from cases a person thought of. The review of 19 November 2026 found "
            "cases the list had missed, so we added a fourth kind: tests that <b>generate</b> the cases.</p>")
    out += "<h2>The approaches</h2>"
    out += table(["Approach", "In plain words", "What it catches", "State"], APPROACHES)
    out += "<h2>A worked example: how a generated test found a history bug</h2>"
    out += ("<p>For history, the question is what silver must hold after a set of changes to a key arrives, in any order, "
            "possibly twice, in any number of batches. The reference model is a few lines. Order a key's changes by "
            "sequence; a new version opens when the tracked value differs from the one before; the last version is "
            "current; a version is deleted when its last change is a delete.</p>")
    out += code(MODEL, r"^def type2_versions", r"^    return out$", "The reference model for SCD Type 2. It is not used to build silver. It is what silver is compared with.", cap=30)
    out += ("<p>The test generates a history (changes to a few keys, delivered out of order, some twice, split into "
            "batches anywhere), builds silver from it, and compares the result with the model. It also checks the "
            "history rules: at most one current version per key, and no overlap.</p>")
    out += code(PROPS, r"^def test_scd2_matches_the_reference_model", 8, "The property test. One test covers every generated history.")
    out += ("<p>Hypothesis found a history of seven changes that made silver disagree with the model. It shrinks a failure "
            "to the smallest history that still fails. Here that is two changes: an update, then a delete that carries a "
            "newer value than the stored version. The delete arrives first, so it overwrites the stored value in place, "
            "and when the missing update arrives later the earlier history cannot be rebuilt.</p>")
    out += code(STAGES, r"a delete that arrives before the update it follows", 9, "The minimal case, kept as a strict known bug: it is expected to fail, and a fix turns it into a failure that says to remove the mark.")
    out += "<h2>What the generated tests found in SCD Type 2</h2>"
    out += table(["#", "What silver does", "Why it matters"], FINDINGS)
    out += ("<p>The SCD Type 1 property test passed every generated history. One question is still open and is a decision "
            "for the owner, not a bug: what a second delete of an already deleted key means. Silver keeps two deleted "
            "versions today and the contract does not say.</p>")
    out += "<h2>How this joins the data twin</h2>"
    out += (f"<p>The twin ({link('tests-world')}) is a generated company with a record of the truth. Today it judges counts, "
            "sums and digests. The new approaches plug into parts it already has: the oracle, the runner's checks, the "
            "world file's incident list, the learning loop that turns a failed run into a scenario file, and the release "
            "gate that compares a changed copy with production.</p>")
    out += table(["Approach", "Where it plugs into the twin", "What is new to build"], PLUG_INS)
    out += ("<p>None of the plug-ins is built yet except the shared model. One honest limit: a simulated company "
            "generates what its author imagines. The learning loop, and eventually real batches, are what stop the "
            "twin from only testing the author's own ideas.</p>")
    out += "<h2>The tiers</h2>"
    out += table(["Tier", "Goal", "Done so far", "Still to do", "Runs on"], TIERS)
    out += ("<p>Order: tier 0, then tier 1 once the owner has decided the three open questions (a second delete of a deleted "
            "key, a delete whose value differs from the stored version, and whether the same changes must give the "
            "same silver however the feed is split), then mutation testing from tier 2, then tiers 3 and 4 together, "
            "then 5 and 6.</p>")
    out += "<h2>The open-source tools</h2>"
    out += table(["Tool", "What it is for", "Where it is used"], IN_USE)
    out += ("<p>Not open source but used: CodeQL and SonarCloud as services in the pipeline. Considered and not adopted: "
            "a mutation-testing tool (none chosen yet), Great Expectations and Soda for data quality (the platform's own "
            "rules run as its own stages), and PyDeequ (used only in a teaching scene). Licences are as the authors state "
            "them; check each before adopting a tool.</p>")
    out += raw("make test-model      # the reference model against every contract case (plain Python, seconds)\n"
               "make test-props      # the generated tests on the fast engine; HYPOTHESIS_MAX_EXAMPLES=300 for more\n"
               "python3 scripts/preflight.py --gate G0     # tools, checkout, no keys in git (see the Deploy page)",
               "How to run them", "Example")
    out += not_built([
        "No test proves the other tests are complete. These approaches raise confidence and show where it is weak.",
        "Mutation testing has not been run, so there is no strength score for any file yet.",
        "The reference model encodes the contract as read today. Where the contract is silent, the model's choice is not a ruling.",
        "The generated tests cover change-feed input only. Full-copy and snapshot input, and the declarative engine, have none.",
        "The data twin does not yet use any of the new approaches, apart from the shared model.",
        "The test dependencies are declared in one file but not yet pinned to exact versions.",
    ])
    return out
