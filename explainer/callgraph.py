"""Who calls what, found by reading the code (no running): the source of the code map's caller tables.

    graph = build(Path("~/data-platform-suite"))
    graph.defs      every function, method and class: id -> Def
    graph.callers   def id -> list of Use (who uses it, and how sure we are)

How a name is resolved, most certain first:
  exact    a plain name defined in the same file, or imported by `from module import name`; `self.method()` inside the
           class that defines the method; `alias.name()` where `alias` is an imported module
  by name  `something.method()` where only the method's name is known: every method with that name is a candidate, so
           the page says "by name" and the reader checks the receiver. Names that are too common to mean anything
           (run, get, write, ...) are listed only when the receiver is `self` or an imported module.
A value is also a use: a function put in a table (`HOPS = {"ingest": run_ingest}`) or passed as an argument is "used as a
value". Tests are counted apart, as `tested_by`.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

ROOTS = ("src/dataplatform", "scripts", "realms")
TOO_COMMON = {"run", "get", "set", "write", "read", "load", "save", "add", "update", "close", "start", "stop", "open",
              "append", "items", "keys", "values", "join", "format", "copy", "pop", "sort", "count", "name", "path",
              "install", "reset", "release", "finish", "main", "frame", "rows", "table", "tables", "compare", "judge"}


@dataclass(frozen=True)
class Def:
    id: str            # file::qualname
    file: str          # path inside the repository
    qual: str          # Class.method or function
    name: str
    kind: str          # function | method | class
    line: int
    doc: str = ""


@dataclass(frozen=True)
class Use:
    by: str            # the Def id (or file::<module> for module level code) that uses it
    file: str
    line: int
    how: str           # exact | by name | value


@dataclass
class Graph:
    defs: dict[str, Def] = field(default_factory=dict)
    callers: dict[str, list[Use]] = field(default_factory=dict)
    tested_by: dict[str, set[str]] = field(default_factory=dict)
    modules: dict[str, dict] = field(default_factory=dict)   # file -> {doc, lines, imports}


def _module_of(rel: str) -> str:
    p = rel[:-3].replace("/", ".")
    for pre in ("src.",):
        if p.startswith(pre):
            p = p[len(pre):]
    return p[:-9] if p.endswith(".__init__") else p


def _files(root: Path, roots=ROOTS):
    for r in roots:
        yield from sorted((root / r).rglob("*.py"))


def _first_line(doc: str | None) -> str:
    return " ".join((doc or "").strip().split("\n\n")[0].split())[:240]


def build(root: Path) -> Graph:
    g = Graph()
    trees: dict[str, ast.Module] = {}
    by_module: dict[str, str] = {}
    for p in _files(root):
        if "__pycache__" in p.parts or ".complexipy_cache" in p.parts:
            continue
        rel = str(p.relative_to(root))
        try:
            tree = ast.parse(p.read_text())
        except (SyntaxError, UnicodeDecodeError):
            continue
        trees[rel] = tree
        by_module[_module_of(rel)] = rel
        g.modules[rel] = {"doc": _first_line(ast.get_docstring(tree)), "lines": len(p.read_text().splitlines())}
    # 1. definitions
    by_name: dict[str, list[Def]] = {}
    by_file_name: dict[tuple[str, str], Def] = {}
    for rel, tree in trees.items():
        def walk(node, cls=None, prefix=""):
            for ch in ast.iter_child_nodes(node):
                if isinstance(ch, ast.ClassDef):
                    d = Def(f"{rel}::{prefix}{ch.name}", rel, f"{prefix}{ch.name}", ch.name, "class", ch.lineno,
                            _first_line(ast.get_docstring(ch)))
                    g.defs[d.id] = d
                    by_name.setdefault(ch.name, []).append(d)
                    by_file_name.setdefault((rel, ch.name), d)
                    walk(ch, ch.name, f"{prefix}{ch.name}.")
                elif isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    kind = "method" if cls else "function"
                    d = Def(f"{rel}::{prefix}{ch.name}", rel, f"{prefix}{ch.name}", ch.name, kind, ch.lineno,
                            _first_line(ast.get_docstring(ch)))
                    g.defs[d.id] = d
                    by_name.setdefault(ch.name, []).append(d)
                    if not cls:
                        by_file_name.setdefault((rel, ch.name), d)
                    walk(ch, None, f"{prefix}{ch.name}.")      # nested functions are named under their parent
                else:
                    walk(ch, cls, prefix)
        walk(tree)
    # 2. uses
    for rel, tree in trees.items():
        from_imports: dict[str, tuple[str, str]] = {}   # local name -> (module file, real name)
        mod_alias: dict[str, str] = {}                   # local alias -> module file
        for n in ast.walk(tree):
            if isinstance(n, ast.ImportFrom) and n.module:
                target = by_module.get(n.module)
                for a in n.names:
                    if target:
                        from_imports[a.asname or a.name] = (target, a.name)
                    else:                                  # `from pkg import module`
                        sub = by_module.get(f"{n.module}.{a.name}")
                        if sub:
                            mod_alias[a.asname or a.name] = sub
            elif isinstance(n, ast.Import):
                for a in n.names:
                    t = by_module.get(a.name)
                    if t:
                        mod_alias[a.asname or a.name.split(".")[-1]] = t
        seen: set[tuple[str, str, str]] = set()

        def add(target: Def, by: str, line: int, how: str) -> None:
            k = (target.id, by, how)
            if k in seen or target.id == by:
                return
            seen.add(k)
            g.callers.setdefault(target.id, []).append(Use(by, rel, line, how))

        def visit(node, owner: str, cls: str | None, prefix: str) -> None:
            for ch in ast.iter_child_nodes(node):
                if isinstance(ch, ast.ClassDef):
                    visit(ch, owner, ch.name, f"{prefix}{ch.name}.")
                    continue
                if isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    visit(ch, f"{rel}::{prefix}{ch.name}", None if cls is None else cls, f"{prefix}{ch.name}.")
                    continue
                if isinstance(ch, ast.Call):
                    use(ch.func, owner, cls, ch.lineno, True)
                    for a in [*ch.args, *[k.value for k in ch.keywords]]:
                        if isinstance(a, (ast.Name, ast.Attribute)):
                            use(a, owner, cls, ch.lineno, False)
                elif isinstance(ch, (ast.Dict, ast.List, ast.Tuple, ast.Set)):
                    for e in ast.iter_child_nodes(ch):
                        if isinstance(e, (ast.Name, ast.Attribute)):
                            use(e, owner, cls, getattr(e, "lineno", 0), False)
                elif isinstance(ch, ast.Assign) and isinstance(ch.value, (ast.Name, ast.Attribute)):
                    use(ch.value, owner, cls, ch.lineno, False)
                visit(ch, owner, cls, prefix)

        def use(f, owner: str, cls: str | None, line: int, called: bool) -> None:
            how_exact = "exact" if called else "value"
            if isinstance(f, ast.Name):
                name = f.id
                if name in from_imports:
                    t_file, real = from_imports[name]
                    d = by_file_name.get((t_file, real))
                    if d:
                        add(d, owner, line, how_exact)
                        return
                d = by_file_name.get((rel, name))
                if d:
                    add(d, owner, line, how_exact)
                return
            if isinstance(f, ast.Attribute):
                name = f.attr
                base = f.value
                if isinstance(base, ast.Name):
                    if base.id in mod_alias:
                        d = by_file_name.get((mod_alias[base.id], name))
                        if d:
                            add(d, owner, line, how_exact)
                        return
                    if base.id in ("self", "cls") and cls:
                        d = g.defs.get(f"{rel}::{cls}.{name}")
                        if d:
                            add(d, owner, line, how_exact)
                            return
                if name in TOO_COMMON or (name.startswith("__") and name.endswith("__")):
                    return
                for d in by_name.get(name, []):
                    if d.kind == "method":
                        add(d, owner, line, "by name" if called else "value")

        visit(tree, f"{rel}::<module>", None, "")
    # 3. tests
    for p in sorted((root / "tests").rglob("*.py")):
        try:
            t = p.read_text()
        except UnicodeDecodeError:
            continue
        rel = str(p.relative_to(root))
        words = set(__import__("re").findall(r"[A-Za-z_][A-Za-z0-9_]*", t))
        for d in g.defs.values():
            if d.name in words and not d.name.startswith("_") and len(d.name) > 4:
                g.tested_by.setdefault(d.id, set()).add(rel)
    return g
