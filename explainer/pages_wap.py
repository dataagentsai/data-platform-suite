"""Write, audit, publish: never show anyone data that has not been checked. Why it matters most when the inputs are not ours."""
from lib import *  # noqa: F401,F403


def picture():
    d = box(10, 50, 170, 80, "New data", ["from the inputs at", "pinned versions"], "bxs")
    d += box(230, 50, 190, 80, "1. Write", ["into a staging table", "nobody reads"], "bx")
    d += box(470, 50, 200, 80, "2. Audit", ["counts, totals, keys,", "known answers"], "bx")
    d += box(720, 50, 200, 80, "3. Publish", ["one atomic swap", "into gold"], "solid")
    d += box(720, 200, 200, 80, "Gold", ["what users read"], "solid")
    d += box(470, 200, 200, 80, "Audit failed", ["nothing is swapped,", "alert is raised"], "bxm")
    d += box(230, 200, 190, 80, "Last good gold", ["keeps being served"], "bx")
    d += arrow(182, 90, 228, 90) + arrow(422, 90, 468, 90) + arrow(672, 90, 718, 90, "passes", lx=695, ly=76)
    d += arrow(820, 132, 820, 198) + arrow(570, 132, 570, 198, "fails", lx=596, ly=170) + arrow(468, 240, 422, 240)
    return d


@page("gold-wap", "Write, audit, publish: never show anyone data that has not been checked", parent="gold",
      short="Write-audit-publish", blurb="Build into a staging table, check it there, and publish by one swap. A failed check changes nothing.")
def wap():
    out = ("<p class='lead'>Write-audit-publish is a way of never showing anyone half-checked data. New gold is built where nobody is "
           "reading, checked there, and only swapped in if the checks pass. If they fail, users keep seeing the last good version "
           "and nobody has to undo anything.</p>")
    out += fig(940, 300, picture(), "Write, audit, publish", "The new data is written to a staging table. The audit decides. Only a pass swaps it into gold.")
    out += "<h2>The three steps</h2>"
    out += table(["Step", "What happens", "In our gold"], [
        ["Write", "The new result is built into a table that no report reads.", "The gold harness builds the windows and dimensions into a staging table, not into gold."],
        ["Audit", "Checks run against the staging table, and nothing else can be affected.", "Row counts and control totals against an independent query on the inputs at the same pinned versions, key checks, no orphaned members, the semantic layer's known answers."],
        ["Publish", "One step makes the staging table the live one. It either happens completely or not at all.", "A swap of what the gold name points to (for example a view pointing to the new table, or a branch fast-forward in Iceberg)."],
    ])
    out += "<h2>Why it matters more when the silver is not ours</h2>"
    out += ("<p>With our own silver, we control the inputs, so a bad input means a bug in our own stages that our tests should catch. "
            f"With a virtual silver ({link('integration-map')}) the inputs are a provider's tables that can change at any time. "
            "The input gate checks the inputs, and write-audit-publish checks the result. Together they mean a provider mistake "
            "stops a run before it reaches a report, and gold keeps serving the last good version.</p>")
    out += ("<p>It is not only for virtual silver. Every gold build benefits: a bug in our own join, a late file, or a wrong mapping is "
            "caught in the audit, not by a user.</p>")
    out += "<h2>Where the idea comes from</h2>"
    out += ("<p>Netflix described it around 2017. It is now common with Apache Iceberg, where a branch plays the part of the staging "
            "table and publishing is a fast-forward of the branch onto the main table. AWS, Dremio and Tabular all document it. "
            "On Delta tables the same effect comes from a staging table and a swap of what the live name points to.</p>")
    out += "<h2>What it does not do</h2>"
    out += table(["Limit", "What it means"], [
        ["It cannot find a rule nobody wrote down", "An audit checks what we thought to check. A wrong business rule in the provider's data passes if no check names it."],
        ["Stopping is still an outage of freshness", "Users see the last good gold, which gets older until someone fixes the cause."],
        ["Several tables published together", "One swap per table is not one swap for all. A set of dimensions and facts needs the swap to be treated as one step, or a catalog that can switch many tables together."],
    ])
    out += not_built([
        "Today the gold harness writes straight into the gold tables, replacing windows in place. A failed audit after that write cannot be undone with one step. So this is not yet write-audit-publish.",
        "There is no staging build, audit step or swap in the platform. It is stage P4 of the virtual silver proof plan.",
        "The audit's checks exist in pieces (reconciliation, key checks, known answers) but not as one gate in front of publishing.",
    ])
    return out
