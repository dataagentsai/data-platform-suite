#!/usr/bin/env python3
"""Preflight: is everything in place before you deploy? Tools, repo, AWS access, Databricks access, CI settings, benchmark.

    python3 scripts/preflight.py --databricks-profile <profile> [--aws-profile <profile>] [--variant aws|free-edition]
                                 [--only tools,repo,aws,databricks,ci,benchmark] [--json]

Read-only: every check lists or describes something, nothing is created or changed. Each check says what failed, why
that matters and the manual step that fixes it (numbered as on the deployment page). Exit code 0 when no check failed;
a `warn` (an optional thing) and a `skip` (a check that needs one that failed) do not fail the run.
Secrets are never printed: a token is reported as set or not set.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
REALM = ROOT / "realms/aws-databricks"
PASS, FAIL, WARN, SKIP = "pass", "fail", "warn", "skip"
_SHOW = 300
NOT_INSTALLED = 127
MAX_SCAN_BYTES = 1_000_000


@dataclass
class Ctx:
    """What the checks may touch. Tests replace `run` and `env`; nothing else reaches the machine."""

    databricks_profile: str = ""
    aws_profile: str = ""
    variant: str = "aws"
    env_name: str = "dev"
    root: Path = ROOT
    env: dict[str, str] = field(default_factory=lambda: dict(os.environ))
    run: Callable[..., tuple[int, str]] = field(default=None)  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.run is None:
            self.run = _run

    def aws(self, *args: str) -> list[str]:
        return ["aws", *(["--profile", self.aws_profile] if self.aws_profile else []), *args]

    def dbx(self, *args: str) -> list[str]:
        return ["databricks", *args, *(["-p", self.databricks_profile] if self.databricks_profile else [])]


def _run(cmd: list[str], cwd: str | None = None) -> tuple[int, str]:
    if shutil.which(cmd[0]) is None:
        return NOT_INSTALLED, f"{cmd[0]}: not installed"
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=60, check=False, cwd=cwd)  # noqa: S603 -- fixed argv, no shell
    except subprocess.TimeoutExpired:
        return 124, "timed out after 60 seconds"
    return p.returncode, (p.stdout + p.stderr).strip()


def short(text: str) -> str:
    """One line, capped, with account numbers and e-mail addresses masked: a check's detail is shown and may be pasted."""
    text = re.sub(r"\b\d{12}\b", "<account>", text)
    text = re.sub(r"[\w.+-]+@[\w-]+(\.[\w-]+)+", "<email>", text)
    return re.sub(r"\s+", " ", text)[:_SHOW]


def version_of(text: str) -> tuple[int, ...]:
    m = re.search(r"(\d+)\.(\d+)(?:\.(\d+))?", text)
    return tuple(int(x) for x in m.groups() if x is not None) if m else ()


@dataclass(frozen=True)
class Check:
    id: str
    group: str
    title: str
    fix: str                      # the manual step that fixes a failure (step numbers: the deployment page)
    test: Callable[[Ctx], tuple[str, str]]  # -> (status, detail)
    needs: tuple[str, ...] = ()   # ids that must pass first, else this one is skipped
    optional: bool = False        # a failure is only a warning
    variants: tuple[str, ...] = ("aws", "free-edition")


# ------------------------------------------------------------------------------------------------------ the tests
def tool(cmd: list[str], minimum: tuple[int, ...] = ()) -> Callable[[Ctx], tuple[str, str]]:
    def t(c: Ctx) -> tuple[str, str]:
        rc, out = c.run(cmd)
        if rc != 0:
            return FAIL, short(out)
        v = version_of(out)
        if minimum and (not v or v < minimum):
            return FAIL, f"found {'.'.join(map(str, v)) or 'an unreadable version'}, need {'.'.join(map(str, minimum))} or newer"
        return PASS, ".".join(map(str, v)) if v else "installed"
    return t


def ok_if_command_works(build: Callable[[Ctx], list[str]]) -> Callable[[Ctx], tuple[str, str]]:
    def t(c: Ctx) -> tuple[str, str]:
        rc, out = c.run(build(c))
        return (PASS, "ok") if rc == 0 else (FAIL, short(out))
    return t


def catalog_checkout(c: Ctx) -> tuple[str, str]:
    d = Path(c.env.get("DP_CATALOG") or Path.home() / "data-platform-catalog")
    if (d / "slices/02-tyre/world.yaml").exists():
        return PASS, d.name
    return FAIL, f"no slices/02-tyre/world.yaml under {d.name}"


def placeholders_filled(c: Ctx) -> tuple[str, str]:
    """Real values belong in <env>.local.tfvars (git-ignored), passed after the tracked file, never in the tracked file."""
    envs = c.root / "realms/aws-databricks/infra/envs"
    local = envs / f"{c.env_name}.local.tfvars"
    if local.exists():
        text = local.read_text()
        if "changeme-dp" in text or re.search(r"\b0{12}\b|0{8}-0{4}-0{4}-0{4}-0{12}", text):
            return FAIL, f"placeholder values left in {local.name}"
        return PASS, f"values come from {local.name} (not tracked by git)"
    return FAIL, f"no {local.name}: copy {c.env_name}.tfvars to it and fill in your values (it is git-ignored)"


# What must never be tracked. Patterns are looked for in every tracked text file; only file names are reported, never the
# matching text.
SECRET_PATTERNS = {
    "an AWS access key id": r"\bAKIA[0-9A-Z]{16}\b",
    "a private key block": r"-----BEGIN [A-Z ]*PRIVATE KEY-----",
    "a Databricks personal access token": r"\bdapi[0-9a-f]{32}\b",
    "a bearer token": r"\bBearer\s+eyJ[A-Za-z0-9_-]{20,}",
    "a password assigned in a file": r"(?i)\b(password|passwd|secret|client_secret)\s*[:=]\s*[\"'][^\"'\s]{8,}[\"']",
}
LOOSE_IN_TESTS = {"a password assigned in a file"}
MUST_IGNORE =("*.local.tfvars", ".env", "*.pem", "*.tfstate", ".databricks/")


def git_ignores_secrets(c: Ctx) -> tuple[str, str]:
    f = c.root / ".gitignore"
    text = f.read_text() if f.exists() else ""
    lines = {ln.strip() for ln in text.splitlines()}
    missing = [p for p in MUST_IGNORE if p not in lines and p.rstrip("/") not in lines]
    return (FAIL, f".gitignore lacks {', '.join(missing)}") if missing else (PASS, "secret file patterns are ignored")


def no_secrets_tracked(c: Ctx) -> tuple[str, str]:
    rc, out = c.run(["git", "-C", str(c.root), "ls-files"])
    if rc != 0:
        return FAIL, short(out)
    hits: dict[str, list[str]] = {}
    for name in out.splitlines():
        p = c.root / name
        try:
            if not p.is_file() or p.stat().st_size > MAX_SCAN_BYTES:
                continue
            text = p.read_text(errors="ignore")
        except OSError:
            continue
        for label, pat in SECRET_PATTERNS.items():
            if label in LOOSE_IN_TESTS and name.startswith("tests/"):
                continue  # test fixtures hold made-up passwords; the other patterns still apply there
            if any("EXAMPLE" not in m.group(0) for m in re.finditer(pat, text)):  # documented example values are not keys
                hits.setdefault(name, []).append(label)
    if hits:
        return FAIL, "; ".join(f"{n} ({', '.join(v)})" for n, v in sorted(hits.items())[:5])
    return PASS, "no key-like text in tracked files"


def gitleaks_clean(c: Ctx) -> tuple[str, str]:
    rc, out = c.run(["gitleaks", "detect", "--source", str(c.root), "--no-banner", "--redact"])
    if rc == NOT_INSTALLED:
        return FAIL, "gitleaks is not installed"
    return (PASS, "no findings in the history") if rc == 0 else (FAIL, "gitleaks reported findings (see its output)")


def env_set(name: str) -> Callable[[Ctx], tuple[str, str]]:
    return lambda c: (PASS, "set") if c.env.get(name) else (FAIL, f"{name} is not set")


def aws_identity(c: Ctx) -> tuple[str, str]:
    rc, out = c.run(c.aws("sts", "get-caller-identity", "--query", "Arn", "--output", "text"))
    if rc != 0:
        return FAIL, short(out)
    return PASS, re.sub(r"\d{12}", "<account>", out.strip())


def probe(service: str, *args: str) -> Callable[[Ctx], tuple[str, str]]:
    """A read-only call that fails when an organisation policy or a missing permission denies the service."""
    def t(c: Ctx) -> tuple[str, str]:
        rc, out = c.run(c.aws(service, *args, "--max-items", "1") if service != "s3api" else c.aws(service, *args))
        return (PASS, "allowed") if rc == 0 else (FAIL, short(out))
    return t


def state_bucket(c: Ctx) -> tuple[str, str]:
    f = c.root / "realms/aws-databricks/infra/envs/account.backend.hcl"
    m = re.search(r'bucket\s*=\s*"([^"]+)"', f.read_text()) if f.exists() else None
    if not m:
        return FAIL, "no bucket named in infra/envs/account.backend.hcl"
    rc, out = c.run(c.aws("s3api", "head-bucket", "--bucket", m.group(1)))
    return (PASS, "state bucket reachable") if rc == 0 else (WARN, "state bucket not reachable yet (make bootstrap creates it)")


def repo_variables(c: Ctx) -> tuple[str, str]:
    rc, out = c.run(["gh", "variable", "list"])
    if rc != 0:
        return FAIL, short(out)
    have = {line.split()[0] for line in out.splitlines() if line.strip()}
    need = {"AWS_REGION", "AWS_PLAN_ROLE_ARN", "DATABRICKS_CLIENT_ID"}
    return (PASS, "all set") if need <= have else (FAIL, f"missing {', '.join(sorted(need - have))}")


def has_sql_warehouse(c: Ctx) -> tuple[str, str]:
    rc, out = c.run(c.dbx("warehouses", "list", "-o", "json"))
    if rc != 0:
        return FAIL, short(out)
    try:
        n = len(json.loads(out or "[]"))
    except ValueError:
        return FAIL, "could not read the warehouse list"
    return (PASS, f"{n} found") if n else (WARN, "none: serverless compute still works, but BI tools need a warehouse")


def lakebase(c: Ctx) -> tuple[str, str]:
    for cmd in (c.dbx("postgres", "list-projects"), c.dbx("database", "list-database-instances")):
        rc, out = c.run(cmd)
        if rc == 0:
            return PASS, "available"
    return WARN, "not offered in this region or tier: set lakebase_enabled = false (metadata store on plain PostgreSQL)"


def lakebase_named(c: Ctx) -> tuple[str, str]:
    """The environment's metadata database exists: a Lakebase project (autoscaling) or an instance (the older shape)."""
    pat = re.compile(rf"(meta-{c.env_name}|{c.env_name}-meta)")
    seen = False
    for cmd in (c.dbx("postgres", "list-projects", "-o", "json"), c.dbx("database", "list-database-instances", "-o", "json")):
        rc, out = c.run(cmd)
        if rc == 0:
            seen = True
            if pat.search(out):
                return PASS, "found"
    return (FAIL, f"no Lakebase project or instance named for {c.env_name} (make free-meta, or the lakebase module)") if seen \
        else (FAIL, "Lakebase cannot be listed with this login")


def bundle_valid(folder: str) -> Callable[[Ctx], tuple[str, str]]:
    def t(c: Ctx) -> tuple[str, str]:
        d = c.root / folder
        if not d.exists():
            return FAIL, f"{folder} not found"
        rc, out = c.run(["databricks", "bundle", "validate", *(["-p", c.databricks_profile] if c.databricks_profile else [])],
                        cwd=str(d))
        return (PASS, "valid") if rc == 0 else (FAIL, short(out))
    return t


def tfvar(c: Ctx, name: str) -> str:
    """A setting's value: the git-ignored local file first, then the tracked one."""
    envs = c.root / "realms/aws-databricks/infra/envs"
    for f in (envs / f"{c.env_name}.local.tfvars", envs / f"{c.env_name}.tfvars"):
        m = re.search(rf'^{name}\s*=\s*"([^"]+)"', f.read_text(), flags=re.M) if f.exists() else None
        if m:
            return m.group(1)
    return ""


def secret_exists(c: Ctx) -> tuple[str, str]:
    prefix = tfvar(c, "name_prefix")
    if not prefix:
        return FAIL, f"no name_prefix in infra/envs/{c.env_name}.tfvars"
    rc, out = c.run(c.aws("secretsmanager", "describe-secret", "--secret-id", f"{prefix}/{c.env_name}/sources"))
    return (PASS, "the secret exists (its value is a separate manual step)") if rc == 0 else (FAIL, short(out))


def named_in(listing: list[str], pattern: str) -> Callable[[Ctx], tuple[str, str]]:
    """Pass when the output of a read-only listing contains a name matching `pattern` (formatted with prefix and env)."""
    def t(c: Ctx) -> tuple[str, str]:
        prefix = tfvar(c, "name_prefix")
        if not prefix:
            return FAIL, f"no name_prefix in infra/envs/{c.env_name}.tfvars"
        rc, out = c.run(c.aws(*listing))
        if rc != 0:
            return FAIL, short(out)
        hits = [w for w in re.split(r"\s+", out) if re.search(pattern.format(prefix=re.escape(prefix), env=c.env_name), w)]
        return (PASS, f"{len(hits)} found") if hits else (FAIL, "none found")
    return t


def catalogs_exist(c: Ctx) -> tuple[str, str]:
    missing = []
    for layer in ("landing", "bronze", "silver", "gold", "ops"):
        rc, _ = c.run(c.dbx("catalogs", "get", f"{c.env_name}_{layer}"))
        if rc != 0:
            missing.append(f"{c.env_name}_{layer}")
    return (FAIL, f"missing {', '.join(missing)}") if missing else (PASS, "all five exist")


def external_locations(c: Ctx) -> tuple[str, str]:
    rc, out = c.run(c.dbx("external-locations", "list", "-o", "json"))
    if rc != 0:
        return FAIL, short(out)
    try:
        n = len(json.loads(out or "[]"))
    except ValueError:
        return FAIL, "could not read the list"
    return (PASS, f"{n} found") if n else (FAIL, "none")


def jobs_exist(c: Ctx) -> tuple[str, str]:
    rc, out = c.run(c.dbx("jobs", "list", "-o", "json"))
    want = f"dp-{c.env_name}-engine-days"
    if rc != 0:
        return FAIL, short(out)
    return (PASS, f"{want} is deployed") if want in out else (FAIL, f"job {want} not found: deploy the engine bundle")


def first_run_left_evidence(c: Ctx) -> tuple[str, str]:
    rc, out = c.run(c.dbx("fs", "ls", f"dbfs:/Volumes/{c.env_name}_ops/reports/runs", "-o", "json"))
    if rc != 0:
        return FAIL, short(out)
    return (PASS, "a run report folder exists") if out.strip() not in ("", "[]") else (FAIL, "no run folder yet: run the job")


CHECKS: tuple[Check, ...] = (
    # tools
    Check("T1", "tools", "Python 3.11 or newer", "install Python (step 0)", tool(["python3", "--version"], (3, 11))),
    Check("T2", "tools", "uv", "install uv (step 0)", tool(["uv", "--version"])),
    Check("T3", "tools", "Terraform 1.10 or newer", "install Terraform (step 0)", tool(["terraform", "version"], (1, 10)),
          variants=("aws",)),
    Check("T4", "tools", "AWS CLI v2", "install the AWS CLI (step 0)", tool(["aws", "--version"], (2, 0))),
    Check("T5", "tools", "Databricks CLI with bundles", "install the Databricks CLI (step 0)",
          tool(["databricks", "--version"], (0, 218))),
    Check("T6", "tools", "rsync", "install rsync (step 0)", tool(["rsync", "--version"])),
    # repo
    Check("R1", "repo", "the specification catalog is checked out", "clone data-platform-catalog beside this repo, or set DP_CATALOG (step 0)",
          catalog_checkout),
    Check("R2", "repo", "your values are in the git-ignored local file", "create <env>.local.tfvars and fill it (step 5)",
          placeholders_filled, variants=("aws",)),
    Check("S1", "repo", "secret file patterns are git-ignored", "add the patterns to .gitignore (step 0)", git_ignores_secrets),
    Check("S2", "repo", "no key-like text in tracked files", "remove it, rotate the key, and keep it in the secret store (step 0)",
          no_secrets_tracked),
    Check("S3", "repo", "gitleaks finds nothing in the history", "install gitleaks, or rotate what it found (step 0)",
          gitleaks_clean, optional=True),
    # aws
    Check("A1", "aws", "signed in to AWS", "aws configure sso, then aws sso login (step 2)", aws_identity, needs=("T4",)),
    Check("A2", "aws", "S3 is allowed", "ask the account owner to lift the policy that denies S3 (step 2)",
          probe("s3api", "list-buckets"), needs=("A1",)),
    Check("A3", "aws", "IAM roles can be listed", "grant IAM read access (step 2)", probe("iam", "list-roles"), needs=("A1",)),
    Check("A4", "aws", "SNS is allowed", "lift the policy that denies SNS (step 2)", probe("sns", "list-topics"), needs=("A1",)),
    Check("A5", "aws", "CloudWatch Logs is allowed", "lift the policy that denies Logs (step 2)",
          probe("logs", "describe-log-groups"), needs=("A1",)),
    Check("A6", "aws", "Secrets Manager is allowed", "lift the policy that denies Secrets Manager (step 2)",
          probe("secretsmanager", "list-secrets"), needs=("A1",), variants=("aws",)),
    Check("A7", "aws", "Kinesis is allowed", "lift the policy that denies Kinesis, or set streams = {} (step 2)",
          probe("kinesis", "list-streams"), needs=("A1",), optional=True),
    Check("A8", "aws", "EC2 and VPC can be read", "lift the policy that denies EC2 (step 2)",
          probe("ec2", "describe-vpcs"), needs=("A1",), variants=("aws",)),
    Check("A9", "aws", "Terraform state bucket", "run make bootstrap (step 7)", state_bucket, needs=("A2",), variants=("aws",)),
    # databricks
    Check("D1", "databricks", "signed in to Databricks", "databricks auth login --profile <name> (step 6)",
          ok_if_command_works(lambda c: c.dbx("current-user", "me")), needs=("T5",)),
    Check("D2", "databricks", "a metastore is assigned", "assign a Unity Catalog metastore to the workspace (step 4)",
          ok_if_command_works(lambda c: c.dbx("metastores", "current")), needs=("D1",)),
    Check("D3", "databricks", "catalogs can be listed", "grant USE CATALOG, or make yourself a metastore admin (step 4)",
          ok_if_command_works(lambda c: c.dbx("catalogs", "list")), needs=("D1",)),
    Check("D4", "databricks", "a SQL warehouse exists", "create one if a BI tool will connect (step 9)", has_sql_warehouse,
          needs=("D1",), optional=True),
    Check("D5", "databricks", "Lakebase is available", "otherwise set lakebase_enabled = false (step 8)", lakebase,
          needs=("D1",), optional=True),
    Check("D6", "databricks", "service principal dp-ci exists", "create it in the account console (step 4)",
          ok_if_command_works(lambda c: c.dbx("service-principals", "list", "--filter", "displayName eq dp-ci")),
          needs=("D1",), optional=True),
    Check("D7", "databricks", "the engine bundle validates", "fix the bundle or the login it names (step 9)",
          bundle_valid("realms/databricks/engine"), needs=("D1",)),
    # ci
    Check("C1", "ci", "signed in to GitHub", "gh auth login (step 11)", ok_if_command_works(lambda c: ["gh", "auth", "status"]),
          optional=True),
    Check("C2", "ci", "repository variables are set", "set them in the repository settings (step 11)", repo_variables,
          needs=("C1",), optional=True),
    # what the deployment should have made (run after make deploy)
    Check("P1", "deployed", "the sources secret exists in Secrets Manager", "run make deploy; then put the value (step 9)",
          secret_exists, needs=("A1",), variants=("aws",)),
    Check("P2", "deployed", "S3 buckets with this prefix exist", "run make deploy (step 9)",
          named_in(["s3api", "list-buckets", "--query", "Buckets[].Name", "--output", "text"], r"^{prefix}"),
          needs=("A2",), variants=("aws",)),
    Check("P3", "deployed", "a KMS key alias with this prefix exists", "run make deploy (step 9)",
          named_in(["kms", "list-aliases", "--query", "Aliases[].AliasName", "--output", "text"], r"{prefix}"),
          needs=("A1",), variants=("aws",)),
    Check("P4", "deployed", "the CI roles exist in IAM", "run make account (step 7)",
          named_in(["iam", "list-roles", "--query", "Roles[].RoleName", "--output", "text"], r"-ci-(plan|{env}|prod)$"),
          needs=("A3",), variants=("aws",)),
    Check("P5", "deployed", "the five catalogs exist", "run make deploy (step 9)", catalogs_exist, needs=("D3",)),
    Check("P6", "deployed", "the landing volume exists", "run make deploy (step 9)",
          ok_if_command_works(lambda c: c.dbx("volumes", "read", f"{c.env_name}_landing.deliveries.files")), needs=("D3",)),
    Check("P7", "deployed", "external locations exist", "run make deploy (step 9)", external_locations,
          needs=("D3",), variants=("aws",)),
    Check("P8", "deployed", "the metadata database (Lakebase) exists", "create it, then run meta_setup.py (step 8)",
          lakebase_named, needs=("D5",)),
    Check("J1", "deployed", "the engine job is deployed", "deploy the engine bundle (step 10)", jobs_exist, needs=("D1",)),
    Check("J2", "deployed", "the first run left its report in the volume", "run the job once (step 11)",
          first_run_left_evidence, needs=("J1",)),
    # benchmark
    Check("B1", "benchmark", "benchmark bundle validates", "fix the bundle or the login it names",
          bundle_valid("benchmarking/databricks"), needs=("D1",)),
    Check("B2", "benchmark", "Power BI token is set", "export PBI_TOKEN before the Power BI run", env_set("PBI_TOKEN"), optional=True),
)


# Stages, in order. Each is a gate: it holds the checks that must pass before the next step of the runbook. `--gate G3`
# runs every check of G0 to G3 and fails if any failed, so a step is never started on a base that is not there.
GATES: tuple[tuple[str, str], ...] = (
    ("G0", "tools and the checkout"), ("G1", "access: AWS, Databricks, GitHub"), ("G2", "after the account setup"),
    ("G3", "after the infrastructure deploy"), ("G4", "after the metadata store setup"),
    ("G5", "after the bundle deploy"), ("G6", "after the first run"),
)
STAGE_OF: dict[str, str] = {
    **dict.fromkeys(("T1", "T2", "T3", "T4", "T5", "T6", "R1", "R2", "S1", "S2", "S3"), "G0"),
    **dict.fromkeys(("A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "D1", "D2", "D3", "D4", "D5", "D6", "C1", "C2"), "G1"),
    **dict.fromkeys(("A9", "P4"), "G2"),
    **dict.fromkeys(("P1", "P2", "P3", "P5", "P6", "P7"), "G3"),
    "P8": "G4",
    **dict.fromkeys(("D7", "B1", "J1"), "G5"),
    "J2": "G6",
    "B2": "G5",
}


def run_checks(ctx: Ctx, only: set[str] | None = None, checks: tuple[Check, ...] = CHECKS,
               gate: str = "") -> list[dict[str, Any]]:
    results: dict[str, str] = {}
    rows: list[dict[str, Any]] = []
    order = [g for g, _ in GATES]
    if gate and gate not in order:
        raise ValueError(f"unknown gate {gate!r}; known: {', '.join(order)}")
    for ch in checks:
        if ctx.variant not in ch.variants:
            continue  # a group filter hides rows but still evaluates what a shown check depends on
        stage = STAGE_OF.get(ch.id, "G1")
        if gate and order.index(stage) > order.index(gate):
            continue
        blocked = [n for n in ch.needs if results.get(n) not in (PASS, WARN)]
        if blocked:
            status, detail = SKIP, f"needs {', '.join(blocked)} first"
        else:
            try:
                status, detail = ch.test(ctx)
            except Exception as exc:  # noqa: BLE001 -- a check that crashes is a failed check, never a crashed preflight
                status, detail = FAIL, f"the check itself failed: {short(str(exc))}"
            if status == FAIL and ch.optional:
                status = WARN
        results[ch.id] = status
        if only and ch.group not in only:
            continue
        rows.append({"id": ch.id, "group": ch.group, "stage": stage, "title": ch.title, "status": status,
                     "detail": detail, "fix": ch.fix if status in (FAIL, WARN) else ""})
    return rows


def render(rows: list[dict[str, Any]]) -> str:
    mark = {PASS: "PASS", FAIL: "FAIL", WARN: "WARN", SKIP: "skip"}
    lines = [f"[{mark[r['status']]}] {r['id']:<3} {r['title']}" + (f": {r['detail']}" if r["detail"] else "") for r in rows]
    for r in rows:
        if r["fix"]:
            lines.append(f"       fix {r['id']}: {r['fix']}")
    n = {s: sum(r["status"] == s for r in rows) for s in (PASS, FAIL, WARN, SKIP)}
    lines.append(f"\n{n[PASS]} passed, {n[FAIL]} failed, {n[WARN]} warnings, {n[SKIP]} skipped")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--databricks-profile", default="")
    ap.add_argument("--aws-profile", default="")
    ap.add_argument("--variant", choices=["aws", "free-edition"], default="aws")
    ap.add_argument("--only", help="comma list of: tools, repo, aws, databricks, ci, deployed, benchmark")
    ap.add_argument("--env", default="dev", help="the environment the deployed-resource checks look for")
    ap.add_argument("--gate", choices=[g for g, _ in GATES], default="",
                    help="run every check up to and including this stage (G0 tools ... G6 first run); fails if any failed")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    if not re.fullmatch(r"[a-z][a-z0-9]{0,15}", a.env):
        ap.error("--env must be a short lower-case name")
    ctx = Ctx(databricks_profile=a.databricks_profile, aws_profile=a.aws_profile, variant=a.variant, env_name=a.env)
    rows = run_checks(ctx, set(a.only.split(",")) if a.only else None, gate=a.gate)
    print(json.dumps(rows, indent=2) if a.json else render(rows))
    return 1 if any(r["status"] == FAIL for r in rows) else 0


if __name__ == "__main__":
    sys.exit(main())
