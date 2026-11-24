"""Build the platform explainer from the repository it lives in. Standard library only, no network.

    python3 explainer/build.py                          -> build/explainer/ (code links are relative paths: works offline)
    python3 explainer/build.py --out DIR                -> DIR
    python3 explainer/build.py --out DIR --links github --github-suite URL --github-catalog URL
                                                        -> code links go to GitHub at each repository's HEAD commit
    options: --home-url URL (a link back to a site), --web-fonts (load fonts from the web; off by default so the output
             makes no outside request), --draft (a link to a page not written yet is dropped, not an error)
    environment: DP_CATALOG (the specification catalog checkout), EXPLAINER_FORBIDDEN_FILE (a file with one regular
                 expression of words that must never appear on a page; optional)

Pages register with @page in pages_*.py. The parent of each page makes the tree, and the tree gives the left
panel, the breadcrumb, the zoom level, the "Look inside" cards and the Back / Next order. The build refuses when a
link points at a page that does not exist, when a code anchor is not found, or when a private word is on a page.
"""
import argparse
import importlib
import os
import re
import sys
from html import escape
from pathlib import Path

import lib
from lib import PAGES, href

import kit

HERE = Path(__file__).resolve().parent
OPTIONS = {"home_url": "", "web_fonts": False}
NAME = "Data Platform"
MODULES = ["pages_start", "pages_map", "pages_meta", "pages_ingest", "pages_sources", "pages_silver", "pages_silver_stages", "pages_silver_history",
           "pages_gold", "pages_wap", "pages_semantic", "pages_obs", "pages_deploy", "pages_tests", "pages_testing", "pages_concerns"]
TABS = [("index", "Start"), ("map", "Code map"), ("concerns", "Concerns"), ("flow", "One day, end to end"), ("meta", "Metadata store"), ("ingest", "Ingestion"),
        ("silver", "Bronze to silver"), ("gold", "Silver to gold"), ("semantic", "Semantic"), ("obs", "Operations"),
        ("deploy", "Deploy"), ("tests", "Tests and proof")]
# Words that must never appear on a page: kept in a file outside the repository, so they are not shipped with it.
_WORDS = os.environ.get("EXPLAINER_FORBIDDEN_FILE")
FORBIDDEN = Path(_WORDS).expanduser().read_text().strip() if _WORDS else ""
EXTRA_CSS = """
.ladder{padding-left:0;list-style:none;counter-reset:s}.ladder li{margin:0 0 8px;padding:8px 14px;border:1.5px solid var(--ink);border-radius:var(--wob);background:var(--paper)}
table.facts th{width:220px;text-align:left;white-space:nowrap}
.zoom{display:inline-block;margin-left:10px;padding:1px 10px;border:1.5px solid var(--ink);border-radius:12px;font-size:14px;letter-spacing:.04em}
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:12px;margin:10px 0 22px}
.cards a{display:block;padding:12px 16px;border:2px solid var(--ink);border-radius:var(--wob);background:var(--paper);text-decoration:none;color:var(--ink)}
.cards a b{display:block;font-family:'Patrick Hand',cursive;font-size:21px}.cards a span{font-size:16px;color:var(--muted)}
.side li.d1{padding-left:12px}.side li.d2{padding-left:24px}.side li.d3{padding-left:36px}.side li.d2 a,.side li.d3 a{font-size:15px}
"""


FIRST = {"index": ["map", "flow"], "silver": ["silver-reader", "silver-plan", "silver-stages", "silver-writer", "silver-declarative",
                    "silver-engines"]}


def children(pid):
    def key(p):
        m = re.match(r"(\d+) ", p["short"])          # "12 History" sorts between 11 and 13
        fixed = FIRST.get(pid, [])
        return (fixed.index(p["id"]) if p["id"] in fixed else len(fixed), int(m.group(1)) if m else 0, p["order"])
    return sorted((p for p in PAGES.values() if p["parent"] == pid), key=key)


def ancestors(pid):
    out = []
    while pid:
        out.append(pid)
        pid = PAGES[pid]["parent"]
    return out[::-1]


def walk(pid=None):
    for c in children(pid):
        yield c["id"]
        yield from walk(c["id"])


def side(cur):
    chain = set(ancestors(cur))
    items = ""
    for p in walk():
        up = ancestors(p)
        depth = len(up) - 1
        # show the top levels always; deeper pages only inside the branch being read
        if depth >= 2 and not (set(up[:-1]) <= chain):
            continue
        items += (f'<li class="d{min(depth, 3)}"><a class="{"on" if p == cur else ""}" href="{href(p)}">'
                  f'{PAGES[p]["short"]}</a></li>')
    return f"<h4>{NAME}</h4><ul>{items}</ul>"


def tabs(cur):
    chain = ancestors(cur)
    hit = next((a for a in reversed(chain) if a in dict(TABS)), None)
    return "".join(f'<a class="{"on" if t == hit else ""}" href="{href(t)}">{n}</a>' for t, n in TABS if t in PAGES)


def pn(cur, order):
    i, s = order.index(cur), ""
    if i > 0:
        p = PAGES[order[i - 1]]
        s += f'<a class="pv" href="{href(p["id"])}"><small>← Back</small>{p["short"]}</a>'
    if i < len(order) - 1:
        n = PAGES[order[i + 1]]
        s += f'<a class="nx" href="{href(n["id"])}"><small>Next →</small>{n["short"]}</a>'
    return f'<nav class="pn">{s}</nav>'


def inside(pid):
    kids = children(pid)
    if not kids:
        return ""
    cards = "".join(f'<a href="{href(k["id"])}"><b>{k["short"]}</b><span>{k["blurb"]}</span></a>' for k in kids)
    return f'<h2>Look inside</h2><div class="cards">{cards}</div>'


def render(pid, order):
    p = PAGES[pid]
    chain = ancestors(pid)
    crumbs = " › ".join([f'<a href="{href(a)}">{PAGES[a]["short"]}</a>' for a in chain[:-1]] + [f'<b>{p["short"]}</b>'])
    level = len(chain) - 1
    plain = escape(re.sub("<[^>]+>", "", p["short"]))
    FONTS = ('<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
             '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Patrick+Hand&family=JetBrains+Mono:wght@500;700&display=swap">'
             if OPTIONS["web_fonts"] else "")
    HOME = f'<a class="home" href="{OPTIONS["home_url"]}">{escape(OPTIONS["home_url"].split("//")[-1].strip("/"))}</a>' if OPTIONS["home_url"] else ""
    desc = escape(re.sub("<[^>]+>", "", p["blurb"] or p["title"]))
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{plain} · {NAME}</title>
<meta name="description" content="{desc}">
{FONTS}<link rel="stylesheet" href="style.css"></head><body>
{kit.DEFS}
<header class="top"><div class="row"><button class="menu" aria-label="Show the page list">☰ Pages</button>
{HOME}<a class="brand" href="index.html">{NAME}, from the big picture to the code</a><nav class="tabs" aria-label="Sections">{tabs(pid)}</nav></div>
<div class="crumbs">You are here: {crumbs}</div></header>
<aside class="side" aria-label="All pages">{side(pid)}</aside>
<main><div class="wrap"><article class="page">
<header><p class="kicker">{NAME}<span class="zoom">Zoom level {level}</span></p><h1>{p['title']}</h1></header>
{lib.starts_box(pid)}
{p['render']()}
{inside(pid)}
</article>{pn(pid, order)}</div></main>
<script src="nav.js"></script></body></html>"""


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=lib.REPO / "build/explainer")
    ap.add_argument("--links", choices=["repo", "github"], default="repo")
    ap.add_argument("--github-suite", default="")
    ap.add_argument("--github-catalog", default="")
    ap.add_argument("--home-url", default="")
    ap.add_argument("--web-fonts", action="store_true")
    ap.add_argument("--draft", action="store_true")
    a = ap.parse_args()
    out = a.out.expanduser().resolve()
    OPTIONS.update(home_url=a.home_url, web_fonts=a.web_fonts)
    lib.LINKS.update(mode=a.links, out=out, github={"P": a.github_suite, "C": a.github_catalog})
    if a.links == "github" and not (a.github_suite and a.github_catalog):
        ap.error("--links github needs --github-suite and --github-catalog")
    only = os.environ.get("SUITE_MODULES")
    for m in (only.split(",") if only else MODULES):
        if (HERE / f"{m}.py").exists():
            importlib.import_module(m)
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*.html"):
        old.unlink(missing_ok=True)
    order = list(walk())
    (out / "style.css").write_text(kit.CSS + EXTRA_CSS)
    (out / "nav.js").write_text(kit.JS)
    for pid in order:
        html = render(pid, order)
        if a.draft:   # a link to a page not written yet becomes plain, so nothing on the page is dead
            html = re.sub(r' href="([a-z0-9-]+)\.html"', lambda m: m.group(0) if m.group(1) in PAGES else "", html)
        (out / href(pid)).write_text(html)
    problems = []
    for f in sorted(out.glob("*.html")):
        t = f.read_text()
        problems += [f"{f.name}: link to missing page {h}" for h in re.findall(r'href="([a-z0-9-]+)\.html"', t)
                     if h not in PAGES]
        body = re.sub(r'href="https://github\.com/[^"]+"', "", t)
        if FORBIDDEN:
            problems += [f"{f.name}: private word '{w}'" for w in set(re.findall(FORBIDDEN, body))]
    if a.draft:
        print(len([x for x in problems if "missing page" in x]), "link(s) to pages not written yet (draft)")
        problems = [x for x in problems if "missing page" not in x]
    problems += [f"code not found: {m}" for m in lib.MISSING]
    orphans = [p for p in PAGES if p not in order]
    problems += [f"page not in the tree: {p}" for p in orphans]
    if problems:
        print(*problems, sep="\n")
        raise SystemExit(f"{len(problems)} problem(s); the explainer is not fit to ship")
    print(f"{len(order)} pages -> {out}")


if __name__ == "__main__":
    main()
