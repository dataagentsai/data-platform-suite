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
