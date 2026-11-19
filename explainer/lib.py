"""Helpers every page of the platform explainer uses. The explainer is generated from the repository it lives in.

    from lib import *      # page, link, box, arrow, text, num, fig, table, note, flow3, code, src, ladder, facts

Rules the helpers enforce:
  - code() cuts real lines from this repository (root P) and the specification catalog (root C) at build time; the
    build fails if an anchor is not found.
  - every code box names file:line and links to it: by relative path inside a checkout (the default, works offline) or
    to GitHub at a pinned commit (links mode "github").
"""
import os
import re
import subprocess
from html import escape
from pathlib import Path

from kit import box, arrow, path, text, num, fig, table, note, flow3  # noqa: F401

HERE = Path(__file__).resolve().parent
REPO = HERE.parent


def _catalog():
    """The specification catalog: DP_CATALOG, else a sibling checkout, else the home folder."""
    if os.environ.get("DP_CATALOG"):
        return Path(os.environ["DP_CATALOG"]).expanduser()
    for c in (REPO.parent / "data-platform-catalog", Path.home() / "data-platform-catalog"):
        if c.exists():
            return c
    return REPO.parent / "data-platform-catalog"


# root letter -> (local folder, repository name)
ROOTS = {"P": (REPO, "data-platform-suite"), "C": (_catalog(), "data-platform-catalog")}
# how code links are written; set by build.py. mode: repo (relative paths from the output folder) | github
LINKS = {"mode": "repo", "out": None, "github": {"P": "", "C": ""}}
MISSING = []
PAGES = {}
_PINS = {}


def page(pid, title, parent=None, blurb="", short=None):
    """Register a page. `parent` makes the tree: left panel, breadcrumb, zoom level, look-inside cards, back/next."""
    def deco(fn):
        assert pid not in PAGES, f"page id used twice: {pid}"
        PAGES[pid] = dict(id=pid, title=title, parent=parent, blurb=blurb, short=short or title, render=fn,
                          order=len(PAGES))
        return fn
    return deco


def href(pid):
    return f"{pid}.html"


def link(pid, text_=None):
    """A link to another page of this explainer, by id (checked at build time)."""
    return f'<a href="{href(pid)}">{text_ or PAGES.get(pid, {}).get("short", pid)}</a>'


def pin(root):
    """The commit the links point at: the local repository's HEAD (must be pushed before publishing)."""
    if root not in _PINS:
        folder = ROOTS[root][0]
        sha = subprocess.run(["git", "-C", str(folder), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
        if LINKS["mode"] == "github":
            on = subprocess.run(["git", "-C", str(folder), "branch", "-r", "--contains", sha], capture_output=True,
                                text=True).stdout
            if "origin/main" not in on:
                raise SystemExit(f"{folder.name}: HEAD {sha[:7]} is not on origin/main; push it before publishing")
        _PINS[root] = sha
    return _PINS[root]


def url(file, line=None):
    root, rel = file.split("/", 1)
    folder, _ = ROOTS[root]
    tail = f"#L{line}" if line else ""
    if LINKS["mode"] == "github":
        return f"{LINKS['github'][root].rstrip('/')}/blob/{pin(root)}/{rel}{tail}"
    out = LINKS["out"] or HERE
    return Path(os.path.relpath(folder / rel, out)).as_posix()


def shown(file):
    root, rel = file.split("/", 1)
    return rel if root == "P" else f"data-platform-catalog/{rel}"


def src(file, text_=None):
    """A plain link to a file (no code shown). file = 'P/src/dataplatform/...' or 'C/slices/...'."""
    root, rel = file.split("/", 1)
    if not (ROOTS[root][0] / rel).exists():
        MISSING.append(file)
    return f'<a href="{url(file)}"><code>{escape(text_ or shown(file))}</code></a>'


def _cut(file, start, end, cap):
    root, rel = file.split("/", 1)
    p = ROOTS[root][0] / rel
    if not p.exists():
        MISSING.append(file)
        return None, 0
    lines = p.read_text().splitlines()
    i = next((k for k, ln in enumerate(lines) if re.search(start, ln)), None)
    if i is None:
        MISSING.append(f"{file}: {start}")
        return None, 0
    while i > 0 and lines[i - 1].lstrip().startswith("@"):
        i -= 1
    if isinstance(end, int):
        j = i + end
    elif isinstance(end, str):
        j = next((k + 1 for k in range(i + 1, len(lines)) if re.search(end, lines[k])), None)
        if j is None:
            MISSING.append(f"{file}: end {end}")
            j = i + cap
    else:                                   # until the block ends (first line back at the same indent)
        ind = len(lines[i]) - len(lines[i].lstrip())
        j, body = i + 1, False
        while j < len(lines):
            ln = lines[j]
            if ln.strip():
                cur = len(ln) - len(ln.lstrip())
                if body and cur <= ind and not ln.strip().startswith((")", "]", "}")):
                    break
                if cur > ind:
                    body = True
            j += 1
    j = min(j, i + cap)
    out = lines[i:j]
    while out and not out[-1].strip():
        out.pop()
    return out, i + 1


def _paint(code_, ext):
    h = escape(code_, quote=False)
    mark = {"py": "#", "yaml": "#", "yml": "#", "sql": "--", "sh": "#", "toml": "#"}.get(ext)
    if mark:
        h = "\n".join(re.sub(rf"((?:^|\s){re.escape(mark)}[^\n]*)$", r'<span class="c">\1</span>', ln)
                      for ln in h.split("\n"))
    return h


def code(file, start, end=None, caption="", cap=60):
    """Real code, cut at build time.
    file    'P/<path in data-platform-suite>' or 'C/<path in data-platform-catalog>'
    start   a regular expression matching the first line to show (use something unique, e.g. r'^def scd2\\(')
    end     None = until that block ends; a number = that many lines; a regular expression = up to and including
            the first later line that matches
    caption one plain sentence saying what to look at
    cap     the most lines to show (default 60)"""
    lines, at = _cut(file, start, end, cap)
    if lines is None:
        return f'<p class="deeper">(code not found: {escape(file)} {escape(str(start))})</p>'
    ext = file.rsplit(".", 1)[-1]
    cap_html = f"<span>{caption}</span>" if caption else ""
    return (f'<div class="snip"><div class="snip-h"><a href="{url(file, at)}"><code>{escape(shown(file))}:{at}</code></a>'
            f'{cap_html}</div><pre>{_paint(chr(10).join(lines), ext)}</pre></div>')


def raw(text_, caption="", kind="Example"):
    """A box of text that is NOT cut from a file (an example table, a worked case). Labelled so nobody mistakes it."""
    cap_html = f"<span>{caption}</span>" if caption else ""
    return (f'<div class="snip"><div class="snip-h"><code>{escape(kind)}</code>{cap_html}</div>'
            f'<pre>{escape(text_, quote=False)}</pre></div>')


def ladder(steps):
    """A numbered list of plain steps: [(short name, sentence), ...]."""
    return '<ol class="ladder">' + "".join(f"<li><b>{a}.</b> {b}</li>" for a, b in steps) + "</ol>"


def facts(rows):
    """A small two-column 'at a glance' table: [(label, value), ...]."""
    return '<div class="tw"><table class="facts">' + "".join(
        f"<tr><th>{a}</th><td>{b}</td></tr>" for a, b in rows) + "</table></div>"


def not_built(items):
    """The honest box every layer page ends with: what is not built yet, or known to be weak."""
    return note("Not built yet, or known gaps", "<ul>" + "".join(f"<li>{i}</li>" for i in items) + "</ul>")


# ------------------------------------------------------------------------------------------ the code map's helpers
_GRAPH = None


def graph():
    """The call graph of the public snapshot, read once (callgraph.py)."""
    global _GRAPH
    if _GRAPH is None:
        import callgraph
        _GRAPH = callgraph.build(ROOTS["P"][0])
    return _GRAPH


def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def def_of(file, name):
    """The definition `name` (a function, class, or Class.method) in a file of the snapshot, or None."""
    rel = file.split("/", 1)[1]
    d = graph().defs.get(f"{rel}::{name}")
    if d is None:
        MISSING.append(f"{file}: {name}")
    return d


def fn(file, name, text_=None):
    """A link to a function or class, showing file:line, e.g. silver/runner.py:69 build_silver."""
    d = def_of(file, name)
    if d is None:
        return f"<code>{escape(name)}</code>"
    short = d.file.replace("src/dataplatform/", "")
    return (f'<a href="{url("P/" + d.file, d.line)}"><code>{escape(text_ or d.qual)}</code></a> '
            f'<small>{escape(short)}:{d.line}</small>')


def caller_links(file, name, limit=5):
    """Who uses a definition, as links; (links html, how many more). Found by reading the code."""
    d = def_of(file, name)
    if d is None:
        return "", 0
    uses = graph().callers.get(d.id, [])
    order = {"exact": 0, "value": 1, "by name": 2}
    uses = sorted(uses, key=lambda u: (order.get(u.how, 3), u.file.startswith("tests"), u.file, u.line))
    out = []
    for u in uses[:limit]:
        who = graph().defs.get(u.by)
        label = (who.qual if who else "module level") + f" · {u.file.replace('src/dataplatform/', '')}:{u.line}"
        mark = "" if u.how == "exact" else f" <small>({u.how})</small>"
        out.append(f'<a href="{url("P/" + u.file, u.line)}"><code>{escape(label)}</code></a>{mark}')
    return "<br>".join(out), max(0, len(uses) - limit)


def starts_box(pid):
    """The box under a layer page's title: where this layer starts in the code and who starts it."""
    rows = STARTS.get(pid)
    if not rows:
        return ""
    all_callers = link("map-callers")
    body = ""
    for label, file, name in rows:
        links, more = caller_links(file, name, 4)
        tail = f" <small>and {more} more, see {all_callers}</small>" if more else ""
        called = (links or "started from outside the code (a job or a command)") + tail
        body += f"<tr><th>{label}</th><td>{fn(file, name)}</td><td>{called}</td></tr>"
    return ('<div class="box"><b>Where this starts in the code</b><div class="tw"><table class="facts">'
            '<tr><th>Part</th><th>Starting point</th><th>Called by (found by reading the code)</th></tr>'
            f"{body}</table></div></div>")


STARTS = {}   # page id -> [(label, file, definition)]; filled by pages_map.py
