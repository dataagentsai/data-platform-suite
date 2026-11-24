"""Source families: how data leaves a system, the products and their routes, and how far each route is proven."""
import json
from html import escape
from pathlib import Path

from lib import *  # noqa: F401,F403

DATA = json.loads((Path(__file__).parent / "data/sources.json").read_text())
FAMILIES = {f["id"]: f for f in DATA["families"]}
SHORT = {"F1": "Files in storage", "F2": "Database, change capture", "F3": "Database, query-based",
         "F4": "Warehouses as sources", "F5": "Shared tables, zero copy", "F6": "Business app APIs",
         "F7": "Event streams", "F8": "Documents and media"}
GROUPS = [("SAP", ["sap"]), ("Salesforce and APIs", ["salesforce", "rest_api"]),
          ("Databases", ["oracle", "sql_server", "postgresql", "mysql"]), ("Warehouses", ["redshift", "snowflake", "bigquery"]),
          ("Files and partners", ["object_storage", "sftp"]), ("Event sources", ["event_streams"]), ("Documents", ["documents"])]
SHAPES = [("files", "Files | plus a manifest", ["F1", "F3", "F4", "F6", "F8"]), ("change_files", "Change files | with operation", ["F2"]),
          ("tables", "Tables | read by version", ["F5"]), ("events", "Events | at least once", ["F7"])]


def best_level(fid):
    return max([r["proof_level"] for p in DATA["products"] for r in p["routes"] if r["family"] == fid] or [0])


def product_families(ids):
    return sorted({r["family"] for p in DATA["products"] if p["id"] in ids for r in p["routes"]})


PRODUCT_SUB = {"SAP": "BDC, APIs, HANA, files", "Salesforce and APIs": "Salesforce, generic REST",
               "Databases": "Oracle, SQL Server, +2", "Warehouses": "Redshift, Snowflake, +1",
               "Files and partners": "Object storage, SFTP", "Event sources": "Kinesis, Event Hubs, Kafka",
               "Documents": "PDFs, images, mail"}


def picture():
    d = text(10, 16, "Products: each a menu of routes", "tx") + text(300, 16, "Families: how data leaves", "tx")
    d += text(632, 16, "What the lander sees", "tx") + text(832, 16, "The platform", "tx")
    gy = {}
    for i, (label, ids) in enumerate(GROUPS):
        y = 30 + i * 76
        gy[label] = y + 31
        d += box(10, y, 215, 62, label, PRODUCT_SUB[label], "bxs")
    fy = {}
    for i, fid in enumerate(FAMILIES):
        y = 30 + i * 66
        fy[fid] = y + 26
        lvl = best_level(fid)
        kind = "solid" if lvl >= 3 else "bx" if lvl >= 1 else "bxm"
        f = FAMILIES[fid]
        d += box(300, y, 290, 52, f"{fid}  {SHORT[fid]}", " or ".join(f["initiators"]).replace("_", " "), kind)
    for label, ids in GROUPS:
        for fid in product_families(ids):
            d += arrow(225, gy[label], 298, fy[fid])
    sy = {}
    for i, (key, label, fids) in enumerate(SHAPES):
        y = 60 + i * 118
        sy[key] = y + 30
        title, _, sub = label.partition(" | ")
        d += box(640, y, 150, 60, title, sub, "bx")
        for fid in fids:
            d += arrow(590, fy[fid], 638, y + 30)
    d += box(835, 150, 160, 130, "Lander", ["contract, quality,", "quarantine, history,", "reconciliation"], "solid", href="ingest-lander.html")
    d += box(835, 310, 160, 62, "Bronze", "pointer, changes, full", "bx")
    d += box(835, 402, 160, 62, "Silver", "versions, current", "bx")
    for key, _, _ in SHAPES:
        d += arrow(790, sy[key], 833, 215)
    d += arrow(915, 281, 915, 308) + arrow(915, 373, 915, 400)
    d += text(10, 572, "Dark box: a route in this family has been run for real.", "tx")
    d += text(10, 594, "Plain: run against a fake or a local copy, or code exists. Dashed: declared only.", "tx")
    return d


@page("ingest-families", "Source families: how data leaves a system, and how far each route is proven", parent="ingest",
      short="Source families", blurb="Eight families of routes, a menu of routes per product, and a proof level for every route.")
def families():
    n_routes = sum(len(p["routes"]) for p in DATA["products"])
    by_level = {lvl: sum(r["proof_level"] == lvl for p in DATA["products"] for r in p["routes"]) for lvl in range(0, 5)}
    out = ("<p class='lead'>Every source system has its own story, but they fall into a few families by how data can leave "
           "them. A family is an <b>access route</b>, not a product. One product offers several routes, each in a different "
           "family, so the useful question is not 'do you support SAP' but 'through which route, and how far has it been "
           "proven'.</p>")
    out += fig(1000, 600, picture(), "Products, source families, arrival shapes and the platform",
               "Read it left to right. Lines join a product to each family it offers a route in. The families feed four "
               "shapes of arrival, which all go through one lander.")
    out += "<h2>The two axes</h2>"
    out += table(["Axis", "What it is", "Example"], [
        ["Family (the route)", "How data can leave a system. The platform implements families.", "A shared table, a file export, an API, a log of changes"],
        ["Product", "A system with a menu of routes and a policy on which to prefer.", "SAP offers a shared table, an API, a file export, a database route and events"],
    ])
    out += ("<p>The mover prefers, in order, a route the vendor certifies, then one that shows changes and deletes, then a "
            "bulk export, then query-based reads. A choice that departs from that order needs a recorded reason in the "
            "dataset's manifest.</p>")
    out += "<h2>The eight families</h2>"
    rows = []
    for f in DATA["families"]:
        q = f["questions"]
        rows.append([f"<b>{f['id']}</b> {escape(f['name'])}", escape(f["arrival"].replace("_", " ")),
                     escape(", ".join(i.replace("_", " ") for i in f["initiators"])), escape(q["changes"]),
                     escape(q["deletes"])])
    out += table(["Family", "Arrives as", "Who starts it", "How changes show", "How deletes show"], rows)
    out += "<h2>The approach for each family, and the fallback</h2>"
    out += ("<p>No single command exists for every product. A warehouse export command is one product's way to meet a "
            "family's need. So each family states the capabilities it needs, the approach, and a slower route that works "
            "almost anywhere when the best one is missing.</p>")
    out += table(["Family", "Capabilities it needs", "Approach", "When the best route is missing", "What its contract test checks"],
                 [[f"<b>{f['id']}</b>", escape(", ".join(c.replace("_", " ") for c in f["needs"])), escape(f["approach"]),
                   escape(f["fallback"]), escape(f["contract_test"])] for f in DATA["families"]])
    out += "<h2>Products and their routes</h2>"
    rows = []
    for p in DATA["products"]:
        rows.append([f"<b>{escape(p['name'])}</b>", f"<i>{escape(p['policy'])}</i>", "", "", ""])
        for r in p["routes"]:
            rows.append(["", escape(r["name"]), r["family"], f"L{r['proof_level']}" + ("" if r["verified"] else " *"),
                         escape(r["evidence"] if r["evidence"] != "none" else r["constraint"])])
    out += table(["Product", "Route", "Family", "Proof", "Evidence, or the constraint when there is none"], rows)
    out += ("<p>An asterisk means the route is written from general knowledge and has not been checked against the "
            "vendor's current documentation. Check it before promising it to anyone.</p>")
    out += "<h2>What 'supported' means: the proof levels</h2>"
    out += table(["Level", "Meaning"], [[f"L{k}", escape(v)] for k, v in DATA["proof_levels"].items()])
    out += (f"<p>Across {n_routes} routes: {by_level[0]} at level 0, {by_level[1]} at level 1, {by_level[2]} at level 2, "
            f"{by_level[3]} at level 3, {by_level[4]} at level 4. A claim of support should name the route and its level.</p>")
    out += not_built([
        "No route is at level 4. One route (files in Azure storage, CSV through the copy service) is at level 3.",
        "The routes marked with an asterisk are not checked against the vendor's current documentation.",
        "Most routes need the owner's setup, licence or permission, which this page does not describe.",
        "The shared-table kind, the warehouse and database movers with an incremental watermark, and the real change-capture proof are not built.",
        f"The data is read from the specification catalog as of {escape(DATA['as_of'])}; the page does not update itself.",
    ])
    return out


# ================================================================================ the integration map: every way in and out
PRODUCTS4 = [("SAP", "#3b82c4"), ("SF", "#27a39a"), ("ORA", "#d9534f"), ("RS", "#e8923a")]
# one row per way in: (title, subtitle, our box kind, target, [SAP, Salesforce, Oracle, Redshift]); V checked this session, K general knowledge
INBOUND = [
    ("Bulk export to files", "F1 F3 F4 · we start it", "solid", "landing", "KKKK"),
    ("Query-based incremental", "F3 F6 · watermark, JDBC or API", "bx", "landing", "KKKK"),
    ("Partner files pushed", "F1 F8 · the sender starts it", "solid", "landing", "KKKK"),
    ("Log-based CDC", "F2 · changes with a sequence", "bx", "bronze", "KKKN"),
    ("Event streams", "F7 · continuous, at least once", "bx", "bronze", "KKKN"),
    ("Managed ingestion connectors", "F6 F2 · vendor pipeline writes tables", "bxm", "bronze", "KVKK"),
    ("Zero-copy sharing", "F5 · shared tables, nothing moves", "bxm", "shared", "VVNN"),
    ("Query federation", "F4 · queried in place, virtual", "bxm", "federated", "NVKK"),
    ("Open tables on shared storage", "F5 · the vendor writes Iceberg", "bxm", "foreign", "NNNV"),
]
OUTBOUND = [
    ("BI and SQL", "Power BI, Tableau, notebooks", "bx", "semantic"), ("AI and Genie", "questions in plain language, models", "bxm", "semantic"),
    ("Delta Sharing out, clean rooms", "share gold, combine without exposing rows", "bxm", "gold"),
    ("Open table interop", "Iceberg REST, UniForm: others read gold", "bxm", "gold"), ("File exports", "Parquet or CSV to storage", "bxm", "gold"),
    ("Reverse ETL", "write results back to applications", "bxm", "gold"), ("Events published", "business events to other systems", "bxm", "gold"),
]


def badge(x, y, state, color):
    if state == "N":
        return f'<circle cx="{x}" cy="{y}" r="4" fill="#bbb"/>'
    fill = color if state == "V" else "#ffffff"
    return f'<circle cx="{x}" cy="{y}" r="11" fill="{fill}" stroke="{color}" stroke-width="3"/>'


def integration_picture():
    d = text(20, 22, "Ways in (each a family of routes)", "tx") + text(535, 22, "Databricks: one governance layer", "tx")
    d += text(1135, 22, "Ways out", "tx")
    for j, (name, color) in enumerate(PRODUCTS4):
        d += f'<text x="{372 + j * 34}" y="40" font-size="14" fill="{color}" text-anchor="middle" font-weight="bold">{name}</text>'
    d += '<rect x="520" y="48" width="560" height="816" rx="18" fill="#f6f1e4" stroke="#222" stroke-width="3"/>'
    d += text(835, 70, "Physical: we store and own it", "tx")
    d += '<rect x="530" y="566" width="262" height="292" rx="12" fill="none" stroke="#555" stroke-width="2" stroke-dasharray="7 6"/>'
    d += text(542, 584, "Virtual: nothing copied", "tx")
    target = {"landing": (835, 90, 235, 70, "Landing", "files and a manifest", "solid"),
              "bronze": (835, 330, 235, 100, "Bronze", "pointer, changes or full", "solid"),
              "silver": (835, 480, 235, 80, "Silver", "derive or conform", "solid"),
              "gold": (835, 610, 235, 80, "Gold", "materialized, we own it", "solid"),
              "semantic": (835, 722, 235, 70, "Semantic", "metric views", "solid"),
              "shared": (542, 596, 238, 70, "Shared catalog", "read-only, pinned version", "bxm"),
              "federated": (542, 682, 238, 70, "Federated catalog", "query in place", "bxm"),
              "foreign": (542, 768, 238, 70, "Foreign catalog", "open tables, Iceberg REST", "bxm")}
    for key, (x, y, w, h, t, s, k) in target.items():
        d += box(x, y, w, h, t, s, k)
    for a, b in (("landing", "bronze"), ("bronze", "silver"), ("silver", "gold"), ("gold", "semantic")):
        xa, ya, _, ha, *_ = target[a]
        _, yb, *_ = target[b]
        d += arrow(952, ya + ha + 1, 952, yb - 1)
    for i, (title, sub, kind, tgt, marks) in enumerate(INBOUND):
        y = 48 + i * 88
        d += box(20, y, 335, 74, title, sub, kind)
        for j, ((_, color), m) in enumerate(zip(PRODUCTS4, marks)):
            d += badge(372 + j * 34, y + 37, m, color)
        tx, ty, tw, th, *_ = target[tgt]
        spread = [0, 18, 36][i % 3] if tgt in ("landing", "bronze") else 35
        d += arrow(460, y + 37, tx - 2, ty + 14 + spread)
    for k in ("shared", "federated", "foreign"):
        _, yy, *_ = target[k]
        d += arrow(782, yy + 35, 833, 630 + ["shared", "federated", "foreign"].index(k) * 22)
    for i, (title, sub, kind, src) in enumerate(OUTBOUND):
        y = 48 + i * 116
        d += box(1135, y, 350, 84, title, sub, kind)
        sx, sy, sw, sh, *_ = target[src]
        d += arrow(1082, sy + 20 + (i % 4) * 12, 1133, y + 42)
    d += text(20, 884, "Filled circle: a route checked against the vendor's documentation this session. Hollow: general knowledge, verify. Grey dot: none known.", "tx")
    d += text(20, 906, "Dark box: built and run for real. Plain: built, or run only on fakes. Dashed: not built.", "tx")
    return d


PLACEMENTS = [
    ["A", "Query in place", "Nothing stored. Each query reads the source.", "None", "None", "Not built"],
    ["B", "Shared in place (virtual silver)", "Nothing stored. Reads a shared table at a pinned version, behind the input gate.", "None", "Provider's history only", "Gate built (P1). Rest not built"],
    ["C", "Pointer bronze", "Only the version, row count and checksum per batch.", "None", "Provider's history only", "Not built"],
    ["D", "Changes-only bronze", "The change rows for a retention window, plus a baseline.", "Small", "Inside the window", "Partly: retention and baseline exist"],
    ["E", "Full bronze", "Landing plus a bronze copy of the raw rows.", "Full", "Always", "Built and run (files, change feed). Streams on fakes"],
    ["F", "Silver: derive", "We build the history from changes or snapshots.", "Full", "Always", "Built and run"],
    ["G", "Silver: conform", "We keep the provider's versions and validate them.", "Full", "Always", "Built for tables we land. Not for shares"],
]
# way in -> the placements that make sense (A..G)
MATRIX = {"Bulk export to files": "DEFG", "Query-based incremental": "DEFG", "Partner files pushed": "DEFG", "Log-based CDC": "DEF",
          "Event streams": "DEF", "Managed ingestion connectors": "CEFG", "Zero-copy sharing": "BCDEFG", "Query federation": "AE",
          "Open tables on shared storage": "BEG"}


@page("integration-map", "The integration map: every way into and out of Databricks, and where the data can sit", parent="ingest",
      short="Integration map", blurb="Nine ways in, seven ways out, four named products, and seven places the data can sit.")
def integration_map():
    out = ("<p class='lead'>This picture puts Databricks in the middle. On the left are the ways data can come in, grouped by "
           "family, with four products placed on them. In the middle is where it can sit. On the right are the ways data "
           "goes out. Most diagrams of this kind show only one product's side. This one shows the choices together.</p>")
    out += fig(1500, 920, integration_picture(), "Every way into and out of Databricks",
               "Read it left to right. Each way in lands in one place inside Databricks. The four coloured circles say which of "
               "SAP, Salesforce, Oracle and Redshift offer that route. Gold feeds every way out.")
    out += ("<p>SAP, Salesforce, Oracle and Redshift are examples. The same rows apply to any product in those families. "
            "A circle is only filled where the route was checked against the vendor's current documentation: SAP and Salesforce "
            "zero-copy sharing, Salesforce's managed connector, Salesforce's query federation, and Redshift writing Iceberg tables. "
            "Everything else is from general knowledge and should be checked before it is promised.</p>")
    out += "<h2>Where the data can sit</h2>"
    out += table(["", "Placement", "What it means", "Copy kept", "Can we replay?", "Our status"], [[f"<b>{r[0]}</b>"] + [escape(c) for c in r[1:]] for r in PLACEMENTS])
    out += ("<p>Gold is not on this list because it is always materialized in our own tables. It is the small, optimized meeting "
            "place where virtual and physical inputs are joined. Publishing it safely is the subject of "+link("gold-wap")+".</p>")
    out += "<h2>Which placements make sense for which way in</h2>"
    heads = ["Way in"] + [f"{p[0]}" for p in PLACEMENTS]
    rows = [[escape(name)] + ["✓" if p[0] in marks else "" for p in PLACEMENTS] for name, marks in MATRIX.items()]
    out += table(heads, rows)
    out += ("<p>A tick means the combination makes sense, not that it is built. For example, federation can only be query in place "
            "(A), or an extract that becomes a full copy (E). A shared table can sit in any of the six placements from B to G.</p>")
    out += not_built([
        "The shared catalog, federated catalog and foreign-catalog placements are not built. Only the input gate for a shared table exists.",
        "The ways out are mostly not built. Metric views ran on Azure and Databricks Free Edition. Sharing out, open-table interop, exports, reverse ETL and published events are designs.",
        "Managed connectors, federation and CDC capture are not wired to the platform.",
        "I have not seen a published diagram that combines ways in, placements and products like this one. Vendors draw their own side, so treat it as our own synthesis, not a standard.",
    ])
    return out
