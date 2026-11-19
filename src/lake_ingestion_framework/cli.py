"""``lake-ingest`` command-line interface.

Each subcommand is a thin entry point; real work lives in
``core.use_cases.*`` modules. See ``docs/code_design.md`` → "Clean
architecture layers" for why the CLI is intentionally small.

For v0.1.0.dev0 most subcommands are stubs that raise
``NotImplementedError`` with a clear message pointing at the relevant
design doc. Implementation lands in subsequent phases.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import typer
from rich.console import Console

from lake_ingestion_framework._version import __version__

if TYPE_CHECKING:
    from lake_ingestion_framework.core.dq import DqRule
    from lake_ingestion_framework.core.lineage import LineageGraph
    from lake_ingestion_framework.core.metadata.repository import DqRuleRow

app = typer.Typer(
    name="lake-ingest",
    help="Metadata-driven framework for ingesting data into a lakehouse.",
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)
config_app = typer.Typer(
    name="config",
    help="Sync customer YAML configs into the metadata DB.",
    no_args_is_help=True,
)
app.add_typer(config_app, name="config")
console = Console()


def _default_metadata_db(customer_repo: Path, env: str) -> Path:
    """Local SQLite path used when ``--metadata-db`` isn't passed.

    Lives under ``.lake-ingest/`` in the customer repo (gitignored by
    the init template). Production realms use Azure SQL DB and pass
    an explicit path/connection string.
    """
    return customer_repo / ".lake-ingest" / f"metadata-{env}.sqlite"


def _framework_root() -> Path:
    """Return the framework's repo root, used to locate templates/.

    For an editable install, ``__file__`` lives inside the source
    checkout, so going up two directories from
    ``src/lake_ingestion_framework/cli.py`` reaches the repo root.
    """
    return Path(__file__).resolve().parents[2]


# ──────────────────────────────────────────────────────────────────────
# Top-level options (--version)
# ──────────────────────────────────────────────────────────────────────


def _version_callback(value: bool) -> None:
    if value:
        console.print(__version__)
        raise typer.Exit()


@app.callback()
def _main(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Print the framework version and exit.",
    ),
) -> None:
    """Top-level callback. The actual subcommands live below."""


# ──────────────────────────────────────────────────────────────────────
# init — scaffold a customer repo from a profile
# ──────────────────────────────────────────────────────────────────────


@app.command()
def init(
    profile: Path = typer.Option(
        ...,
        "--profile",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Customer profile YAML (see templates/profiles/ for examples).",
    ),
    target_dir: Path | None = typer.Option(
        None,
        "--target-dir",
        help="Directory to scaffold into. Defaults to '<customer.name>-ingestion/'.",
    ),
    apply: bool = typer.Option(False, "--apply", help="Actually write files (default: dry-run)."),
    update: bool = typer.Option(
        False,
        "--update",
        help="Overwrite an existing target directory.",
    ),
    framework_path: Path | None = typer.Option(
        None,
        "--framework-path",
        help="Local checkout path for editable-install pin in customer's pyproject.toml. "
        "Defaults to this framework's repo root.",
    ),
) -> None:
    """Scaffold a customer repo from a profile YAML.

    Dry-run by default. Use ``--apply`` to write files. See
    ``docs/customer_profile.md`` and ``docs/local_install.md``.
    """
    from lake_ingestion_framework.core.use_cases import init as init_use_case

    template_root = _framework_root() / "templates" / "customer_repo_template"
    if not template_root.is_dir():
        raise typer.BadParameter(
            f"Template tree not found at {template_root}. "
            "Run from a framework checkout where templates/ exists."
        )

    plan = init_use_case.plan_init(
        profile_path=profile,
        target_dir=target_dir,
        template_root=template_root,
        framework_local_path=framework_path or _framework_root(),
    )

    source_types = sorted({s.source_type.value for s in plan.profile.sources})
    env_names = [e.name for e in plan.profile.environments]
    console.print(
        f"[bold]Customer:[/bold] {plan.profile.customer.display_name} "
        f"({plan.profile.customer.name})"
    )
    console.print(f"[bold]Target:[/bold] {plan.target_dir}")
    console.print(f"[bold]Sources:[/bold] {', '.join(source_types)}")
    console.print(f"[bold]Environments:[/bold] {', '.join(env_names)}")
    console.print()

    console.print(f"[bold]Files ({len(plan.files)}):[/bold]")
    for pf in plan.files:
        try:
            display = pf.target_path.relative_to(plan.target_dir)
        except ValueError:
            display = pf.target_path
        console.print(f"  • {display}")

    if not apply:
        console.print()
        console.print("[yellow]Dry run.[/yellow] Re-run with --apply to write.")
        return

    init_use_case.apply_init(plan, overwrite=update)
    console.print()
    console.print(f"[green]Scaffolded {len(plan.files)} files into {plan.target_dir}[/green]")


# ──────────────────────────────────────────────────────────────────────
# config sync — push customer YAML config to metadata DB
# ──────────────────────────────────────────────────────────────────────


@config_app.command("sync")
def config_sync(
    env: str = typer.Option(..., "--env", help="Environment (dev / uat / prod / ...)."),
    customer_repo: Path = typer.Option(
        Path.cwd(),
        "--customer-repo",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        help="Customer repo root (defaults to CWD).",
    ),
    metadata_db: Path | None = typer.Option(
        None,
        "--metadata-db",
        help="SQLite metadata DB path. Defaults to "
        "<customer-repo>/.lake-ingest/metadata-<env>.sqlite.",
    ),
    apply: bool = typer.Option(False, "--apply", help="Actually write (default: dry-run)."),
    delete_missing_dq_rules: bool = typer.Option(
        False,
        "--delete-missing-dq-rules",
        help="Hard-delete DQ rules in the DB that aren't in any YAML "
        "(default: soft-delete via is_enabled=0 to keep the audit trail).",
    ),
) -> None:
    """Sync ``config/platform_config/<env>.yaml`` + ``config/sources/`` +
    ``config/datasets/`` + ``config/dq/`` into the metadata DB.

    Dry-run by default — prints the planned upsert counts. Use ``--apply``
    to write. See ``docs/customer_profile.md``.
    """
    from lake_ingestion_framework.core.metadata.repository import SqliteMetadataRepository
    from lake_ingestion_framework.core.use_cases.config_sync import (
        apply_config_sync,
        plan_config_sync,
    )

    plan = plan_config_sync(customer_repo=customer_repo, env=env)

    console.print(f"[bold]Environment:[/bold] {plan.env}")
    console.print(f"[bold]Customer repo:[/bold] {customer_repo}")
    pc_total = len(plan.platform_config.kv) + len(plan.platform_config.processor_groups)
    pii_count = len(plan.platform_config.pii_policy)
    console.print(
        f"[bold]platform_config:[/bold] {pc_total} keys "
        f"({plan.platform_config_path.relative_to(customer_repo)})"
        + (f" + {pii_count} pii_policy entries" if pii_count else "")
    )
    console.print(f"[bold]sources:[/bold] {len(plan.sources)}")
    for src in plan.sources:
        console.print(
            f"  • {src.source_name} ({src.source_env}) — "
            f"{src.source_type.value}, {len(src.config)} config keys"
        )
    console.print(f"[bold]datasets:[/bold] {len(plan.datasets)}")
    for ds in plan.datasets:
        cols_note = f", {len(ds.columns)} cols" if ds.columns else ""
        console.print(
            f"  • {ds.source_name}/{ds.dataset_name} ({ds.source_env}) — "
            f"{len(ds.config)} config keys{cols_note}"
        )
    if plan.dq_rule_sets:
        rules_total = sum(len(rs.rules) for rs in plan.dq_rule_sets)
        console.print(
            f"[bold]dq rule sets:[/bold] {len(plan.dq_rule_sets)} "
            f"({rules_total} rules across all files)"
        )
        for rs in plan.dq_rule_sets:
            console.print(f"  • {rs.layer.value}/{rs.dataset_name} — {len(rs.rules)} rules")
    for warning in plan.warnings:
        console.print(f"[yellow]warning:[/yellow] {warning}")
    for cw in plan.dq_warnings:
        console.print(
            f"[yellow]cheap-eval warning:[/yellow] rule {cw.rule_name!r}: {cw.suggestion}"
        )

    if not apply:
        console.print()
        console.print("[yellow]Dry run.[/yellow] Re-run with --apply to write to the metadata DB.")
        return

    db_path = metadata_db or _default_metadata_db(customer_repo, env)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    repo = SqliteMetadataRepository(db_path)
    try:
        result = apply_config_sync(plan, repo, delete_missing_dq_rules=delete_missing_dq_rules)
    finally:
        repo.close()

    console.print()
    console.print(
        f"[green]Synced[/green] {result.platform_config_keys} platform_config keys, "
        f"{result.sources_upserted} sources, {result.datasets_upserted} datasets "
        f"into {db_path}"
    )
    if (
        result.dq_rules_inserted
        or result.dq_rules_updated
        or result.dq_rules_unchanged
        or result.dq_rules_soft_deleted
        or result.dq_rules_hard_deleted
    ):
        console.print(
            f"[green]DQ rules:[/green] {result.dq_rules_inserted} inserted, "
            f"{result.dq_rules_updated} updated, "
            f"{result.dq_rules_unchanged} unchanged, "
            f"{result.dq_rules_soft_deleted} soft-deleted, "
            f"{result.dq_rules_hard_deleted} hard-deleted"
        )


# ──────────────────────────────────────────────────────────────────────
# discover — auto-populate metadata from a registered source
# ──────────────────────────────────────────────────────────────────────


@app.command()
def discover(
    source: str = typer.Option(..., "--source", help="Source name from source_master."),
    env: str = typer.Option(
        "prod",
        "--env",
        help="source_env for the source (defaults to 'prod').",
    ),
    customer_repo: Path = typer.Option(
        Path.cwd(),
        "--customer-repo",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        help="Customer repo root (defaults to CWD).",
    ),
    metadata_db: Path | None = typer.Option(
        None,
        "--metadata-db",
        help="SQLite metadata DB path. Defaults to "
        "<customer-repo>/.lake-ingest/metadata-<env>.sqlite.",
    ),
    schema_filter: list[str] | None = typer.Option(
        None,
        "--schema",
        help="Limit walk to these schemas (repeatable). Default: all user schemas.",
    ),
    apply: bool = typer.Option(False, "--apply", help="Write to metadata DB (default: dry-run)."),
    output: Path | None = typer.Option(
        None,
        "--output",
        file_okay=False,
        dir_okay=True,
        help="Also write proposed dataset YAMLs to this directory.",
    ),
) -> None:
    """Auto-discover schema metadata from a registered source.

    Read-only — never writes to the source. New datasets land with
    ``is_enabled=False``. See ``docs/discovery.md``.
    """
    from lake_ingestion_framework.core.metadata.repository import SqliteMetadataRepository
    from lake_ingestion_framework.core.source_connectors._secret_resolver import (
        LocalEnvSecretResolver,
    )
    from lake_ingestion_framework.core.use_cases.discover import (
        apply_discover,
        plan_discover,
        write_proposal_yamls,
    )

    db_path = metadata_db or _default_metadata_db(customer_repo, env)
    if not db_path.is_file():
        raise typer.BadParameter(
            f"Metadata DB not found at {db_path}. Run `lake-ingest config sync --apply` first."
        )

    repo = SqliteMetadataRepository(db_path)
    resolver = LocalEnvSecretResolver()

    schemas: tuple[str, ...] | None = tuple(schema_filter) if schema_filter else None

    try:
        proposal = plan_discover(
            source_name=source,
            source_env=env,
            repo=repo,
            secret_resolver=resolver,
            schemas=schemas,
        )

        console.print(f"[bold]Source:[/bold] {proposal.source_name} (env={proposal.source_env})")
        console.print(f"[bold]Schemas seen:[/bold] {', '.join(proposal.schemas_seen) or '(none)'}")
        console.print(f"[bold]Datasets proposed:[/bold] {len(proposal.datasets)}")

        for ds in proposal.datasets:
            wm = ds.watermark_column or "[red](none — FULL load)[/red]"
            console.print(
                f"  • {ds.dataset_name} ({ds.schema_name}.{ds.object_name}) — "
                f"{ds.dataset_type.value}, {ds.load_type.value}, watermark={wm}, "
                f"{len(ds.columns)} cols"
            )
            for warning in ds.warnings:
                console.print(f"      [yellow]warning:[/yellow] {warning}")

        for skipped in proposal.skipped:
            console.print(f"[yellow]skipped:[/yellow] {skipped}")

        if output is not None:
            written = write_proposal_yamls(proposal, output)
            console.print()
            console.print(f"[green]Wrote[/green] {len(written)} dataset YAMLs to {output}")

        if not apply:
            console.print()
            console.print(
                "[yellow]Dry run.[/yellow] Re-run with --apply to write proposals to the "
                "metadata DB (datasets land with is_enabled=False)."
            )
            return

        result = apply_discover(proposal, repo)
        console.print()
        console.print(
            f"[green]Wrote[/green] {result.datasets_upserted} datasets, "
            f"{result.columns_upserted} columns to {db_path}. "
            "Datasets are disabled by default — review and flip is_enabled."
        )
    finally:
        repo.close()


# ──────────────────────────────────────────────────────────────────────
# deploy — generate realm artifacts from metadata
# ──────────────────────────────────────────────────────────────────────


@app.command()
def deploy(
    realm: str = typer.Option(..., "--realm", help="Target realm name."),
    env: str = typer.Option(..., "--env", help="Environment (dev / uat / prod / ...)."),
    customer_repo: Path = typer.Option(
        Path.cwd(),
        "--customer-repo",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        help="Customer repo root (defaults to CWD).",
    ),
    metadata_db: Path | None = typer.Option(
        None,
        "--metadata-db",
        help="SQLite metadata DB path. Defaults to "
        "<customer-repo>/.lake-ingest/metadata-<env>.sqlite.",
    ),
    apply: bool = typer.Option(False, "--apply", help="Actually write files (default: dry-run)."),
    include_procs: bool = typer.Option(
        False,
        "--include-procs",
        help=(
            "Also emit runtime SQL artifacts (views, log tables, stored "
            "procedures) under runtime_sql/. Apply with sqlcmd. See "
            "docs/realms/adf-adls-runtime-sql.md."
        ),
    ),
) -> None:
    """Generate realm-native artifacts (ADF JSON, SQL, etc.) from metadata.

    Dry-run by default; use ``--apply`` to write under
    ``<customer-repo>/generated/<realm>/<env>/``. Output is byte-
    identical for the same metadata (per the golden-diff invariant
    in CLAUDE.md). See ``docs/realms/adf-adls-pipelines.md``.
    """
    from lake_ingestion_framework.core.metadata.repository import SqliteMetadataRepository
    from lake_ingestion_framework.core.use_cases.deploy import (
        apply_deploy,
        plan_deploy,
    )

    db_path = metadata_db or _default_metadata_db(customer_repo, env)
    if not db_path.is_file():
        raise typer.BadParameter(
            f"Metadata DB not found at {db_path}. Run `lake-ingest config sync --apply` first."
        )

    repo = SqliteMetadataRepository(db_path)
    try:
        plan = plan_deploy(
            realm=realm,
            env=env,
            repo=repo,
            customer_repo=customer_repo,
            include_procs=include_procs,
        )

        console.print(f"[bold]Realm:[/bold] {plan.realm}")
        console.print(f"[bold]Environment:[/bold] {plan.env}")
        console.print(f"[bold]Target:[/bold] {plan.target_dir}")
        console.print(f"[bold]Artifacts ({len(plan.files)}):[/bold]")

        kind_counts: dict[str, int] = {}
        for pf in plan.files:
            kind_counts[pf.kind] = kind_counts.get(pf.kind, 0) + 1
            try:
                display = pf.target_path.relative_to(plan.target_dir)
            except ValueError:
                display = pf.target_path
            console.print(f"  • {pf.kind}: {display}")

        for warning in plan.warnings:
            console.print(f"[yellow]warning:[/yellow] {warning}")

        if not apply:
            console.print()
            console.print("[yellow]Dry run.[/yellow] Re-run with --apply to write artifacts.")
            return

        result = apply_deploy(plan)
        console.print()
        breakdown = ", ".join(f"{k}={v}" for k, v in sorted(result.by_kind.items()))
        console.print(
            f"[green]Wrote[/green] {result.files_written} files ({breakdown}) to {plan.target_dir}"
        )
    finally:
        repo.close()


# ──────────────────────────────────────────────────────────────────────
# doctor — health probe of a deployed environment
# ──────────────────────────────────────────────────────────────────────


@app.command()
def doctor(
    env: str = typer.Option(..., "--env", help="Environment to probe."),
    customer_repo: Path = typer.Option(
        Path.cwd(),
        "--customer-repo",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        help="Customer repo root (defaults to CWD).",
    ),
    metadata_db: Path | None = typer.Option(
        None,
        "--metadata-db",
        help="SQLite metadata DB path. Defaults to "
        "<customer-repo>/.lake-ingest/metadata-<env>.sqlite.",
    ),
) -> None:
    """Health-probe a deployed environment.

    Runs realm-agnostic probes: framework version, config schema,
    metadata DB connectivity. Realm-specific probes (Key Vault, RBAC,
    ADLS write) plug in at deploy time. See ``docs/operability.md``.
    """
    from lake_ingestion_framework.core.metadata.repository import SqliteMetadataRepository
    from lake_ingestion_framework.core.probes import ProbeStatus
    from lake_ingestion_framework.core.use_cases.doctor import run_doctor

    db_path = metadata_db or _default_metadata_db(customer_repo, env)
    repo = SqliteMetadataRepository(db_path)
    try:
        report = run_doctor(customer_repo=customer_repo, env=env, metadata_repo=repo)
    finally:
        repo.close()

    icon = {
        ProbeStatus.PASS: "[green]✓[/green]",
        ProbeStatus.WARN: "[yellow]![/yellow]",
        ProbeStatus.FAIL: "[red]✗[/red]",
        ProbeStatus.SKIPPED: "[dim]·[/dim]",
    }
    for result in report.results:
        console.print(f"{icon[result.status]} {result.name}: {result.summary}")
        for detail in result.details:
            console.print(f"    {detail}")

    console.print()
    overall = report.overall
    label = {
        ProbeStatus.PASS: "[green]all checks passed[/green]",
        ProbeStatus.WARN: "[yellow]warnings present[/yellow]",
        ProbeStatus.FAIL: "[red]failures present[/red]",
        ProbeStatus.SKIPPED: "[dim]all skipped[/dim]",
    }[overall]
    console.print(label)

    if overall is ProbeStatus.FAIL:
        raise typer.Exit(code=1)


# ──────────────────────────────────────────────────────────────────────
# reingest — overwrite specific load_date partition(s)
# ──────────────────────────────────────────────────────────────────────


@app.command()
def reingest(
    dataset: str = typer.Option(..., "--dataset", help="Dataset name (or comma-separated list)."),
    load_date: str | None = typer.Option(None, "--load-date", help="A single YYYY-MM-DD."),
    from_date: str | None = typer.Option(None, "--from", help="Start of date range (inclusive)."),
    to_date: str | None = typer.Option(None, "--to", help="End of date range (inclusive)."),
    apply: bool = typer.Option(False, "--apply", help="Actually run (default: dry-run)."),
) -> None:
    """Re-ingest specific load_date partition(s) with current source state.

    See ``docs/recovery.md`` → "reingest".
    """
    raise NotImplementedError(
        "lake-ingest reingest: implementation coming in the next phase. "
        "See docs/recovery.md for the design."
    )


# ──────────────────────────────────────────────────────────────────────
# backfill — ingest a historical window
# ──────────────────────────────────────────────────────────────────────


@app.command()
def backfill(
    dataset: str = typer.Option(..., "--dataset", help="Dataset name."),
    data_from: str = typer.Option(..., "--data-from", help="Start of data-date window."),
    data_to: str = typer.Option(..., "--data-to", help="End of data-date window."),
    strategy: str = typer.Option(
        "single-partition", "--strategy", help="single-partition | per-day-partition."
    ),
    apply: bool = typer.Option(False, "--apply", help="Actually run (default: dry-run)."),
) -> None:
    """Ingest a historical window not previously covered.

    See ``docs/recovery.md`` → "backfill".
    """
    raise NotImplementedError(
        "lake-ingest backfill: implementation coming in the next phase. "
        "See docs/recovery.md for the design."
    )


# ──────────────────────────────────────────────────────────────────────
# reset-watermark — operator-driven rollback (audited)
# ──────────────────────────────────────────────────────────────────────


@app.command()
def run(
    pipeline: str = typer.Option(
        ..., "--pipeline", help="Pipeline name to execute (e.g., PL_INGEST_POSTGRES)."
    ),
    mode: str = typer.Option(
        "local", "--mode", help="Execution mode (local | cloud). Cloud is deferred."
    ),
    artifacts_root: Path = typer.Option(
        ...,
        "--artifacts-root",
        help="Path to deployed ADF artifacts (containing pipelines/, datasets/, linkedServices/).",
    ),
    metadata_db: Path = typer.Option(
        ..., "--metadata-db", help="Path to the local SQLite metadata DB."
    ),
    lakes_root: Path = typer.Option(
        Path.home() / ".lake-ingest" / "lakes",
        "--lakes-root",
        help="Root directory for local Bronze Delta tables.",
    ),
    param: list[str] = typer.Option(
        [], "--param", help="Pipeline parameter, key=value (repeatable).", show_default=False
    ),
) -> None:
    """Run a pipeline locally end-to-end via the ADF Mimic.

    Reads ADF artifacts from ``--artifacts-root``, executes against the
    local SQLite metadata DB, and writes Bronze Delta into ``--lakes-root``.
    Per ``docs/adf_mimic_design.md``.
    """
    if mode != "local":
        raise typer.BadParameter(
            f"--mode={mode!r} not supported. v1 supports 'local' only; cloud uses ADF directly."
        )

    parameters: dict[str, str] = {}
    for entry in param:
        if "=" not in entry:
            raise typer.BadParameter(f"--param expects key=value, got {entry!r}.")
        key, _, value = entry.partition("=")
        parameters[key] = value

    from lake_ingestion_framework.core.run import run_pipeline_local

    report = run_pipeline_local(
        pipeline_name=pipeline,
        artifacts_root=artifacts_root,
        metadata_db=metadata_db,
        lakes_root=lakes_root,
        parameters=parameters,
    )

    typer.echo(f"Pipeline: {report.pipeline.pipeline_name}")
    typer.echo(f"Run ID:   {report.pipeline.run_id}")
    typer.echo(f"Status:   {report.pipeline.status}")
    for r in report.pipeline.activity_results:
        typer.echo(f"  - [{r.status}] {r.output if r.status == 'Succeeded' else r.error}")
    if not report.succeeded:
        raise typer.Exit(code=1)


silver_app = typer.Typer(name="silver", help="Bronze → Silver pipeline.", no_args_is_help=True)
app.add_typer(silver_app, name="silver")


@silver_app.command("run")
def silver_run(
    bronze: Path = typer.Option(..., "--bronze", help="Bronze Delta path (input)."),
    silver: Path = typer.Option(..., "--silver", help="Silver Delta path (output)."),
    run_id: str = typer.Option(
        ..., "--run-id", help="Run ID stamped into _silver_run_id on every row."
    ),
    strategy: str = typer.Option(
        "append",
        "--strategy",
        help="Write strategy: append | scd1 | scd2 | upsert.",
    ),
    business_keys: list[str] = typer.Option(
        [],
        "--business-key",
        help="Natural-key column for SCD1 / SCD2 MERGE predicate (repeatable).",
    ),
    hash_columns: list[str] = typer.Option(
        [],
        "--hash-column",
        help="Column participating in _record_hash (repeatable). "
        "Default: every non-metadata source column.",
    ),
    partition_by: list[str] = typer.Option(
        [], "--partition-by", help="Partition column (repeatable)."
    ),
    scd2_order_by: str | None = typer.Option(
        None,
        "--scd2-order-by",
        help="Source column whose value populates _valid_from / _valid_to. "
        "Required when --strategy=scd2.",
    ),
    lineage_output: Path | None = typer.Option(
        None,
        "--lineage-output",
        help="Append one OpenLineage COMPLETE event for this run to the given "
        "JSONL file. Requires the [lineage] extra. Skipped silently if absent.",
    ),
    lineage_namespace: str = typer.Option(
        "delta://lakehouse",
        "--lineage-namespace",
        help="OL dataset namespace (e.g., 'databricks://workspace.cloud.databricks.com').",
    ),
    lineage_silver_name: str | None = typer.Option(
        None,
        "--lineage-silver-name",
        help="Silver-side dataset name in OL events (default: <silver-path>.name). "
        "Pass 'silver.<table>' to match dbt's manifest convention so the Bronze→"
        "Silver→Gold graph stitches.",
    ),
    lineage_bronze_name: str | None = typer.Option(
        None,
        "--lineage-bronze-name",
        help="Bronze-side dataset name in OL events (default: <bronze-path>.name).",
    ),
) -> None:
    """Run the Bronze → Silver pipeline locally.

    Reads the Bronze Delta table, drops Bronze tracking metadata, recomputes
    the per-row hash, stamps Silver metadata columns, and writes to the
    Silver Delta table per the chosen strategy.
    """
    from lake_ingestion_framework.core.silver import (
        SilverConfig,
        SilverPipeline,
        SilverStrategy,
    )

    try:
        strategy_enum = SilverStrategy(strategy)
    except ValueError as exc:
        raise typer.BadParameter(
            f"Unknown --strategy {strategy!r}. "
            f"Supported: {', '.join(s.value for s in SilverStrategy)}."
        ) from exc

    stats = SilverPipeline().run(
        SilverConfig(
            bronze_path=bronze,
            silver_path=silver,
            run_id=run_id,
            strategy=strategy_enum,
            business_keys=tuple(business_keys) if business_keys else None,
            hash_columns=tuple(hash_columns) if hash_columns else None,
            partition_by=tuple(partition_by) if partition_by else None,
            scd2_order_by=scd2_order_by,
        )
    )

    if lineage_output is not None:
        from lake_ingestion_framework.core.lineage import (
            JsonLinesSink,
            emit_silver_run_event,
        )

        with JsonLinesSink(lineage_output, mode="a") as sink:
            emit_silver_run_event(
                stats,
                sink,
                namespace=lineage_namespace,
                silver_dataset_name=lineage_silver_name,
                bronze_dataset_name=lineage_bronze_name,
            )

    typer.echo(f"Strategy:        {stats.strategy.value}")
    typer.echo(f"Rows read:       {stats.rows_read}")
    typer.echo(f"Rows written:    {stats.rows_written}")
    typer.echo(f"Rows inserted:   {stats.rows_inserted}")
    typer.echo(f"Rows updated:    {stats.rows_updated}")
    if stats.strategy == SilverStrategy.SCD2:
        typer.echo(f"Rows unchanged:  {stats.rows_unchanged}")
    if stats.silver_version_after is not None:
        typer.echo(f"Silver version:  {stats.silver_version_after}")
    typer.echo(f"Duration:        {stats.duration_seconds:.3f}s")
    typer.echo(f"Silver path:     {stats.silver_path}")
    if lineage_output is not None:
        typer.echo(f"Lineage event:   appended to {lineage_output}")


gold_app = typer.Typer(
    name="gold",
    help="Silver → Gold dbt project generator.",
    no_args_is_help=True,
)
app.add_typer(gold_app, name="gold")


@gold_app.command("deploy")
def gold_deploy(
    config: Path = typer.Option(..., "--config", help="YAML config file (GoldProjectConfig)."),
    output: Path = typer.Option(..., "--output", help="Output directory for the dbt project."),
) -> None:
    """Generate a dbt project from a YAML config.

    Phase 1 takes a customer-authored YAML; later phases hydrate from
    ``meta.dataset_config[layer='gold']`` directly. Re-running produces
    byte-identical output for the same input (golden-diff ready).
    """
    from lake_ingestion_framework.core.gold import (
        generate_dbt_project,
        load_project_config,
    )

    project = load_project_config(config)
    stats = generate_dbt_project(project, output)

    typer.echo(f"Project name:    {stats.project_name}")
    typer.echo(f"Sources:         {stats.sources_count}")
    typer.echo(f"Models:          {stats.models_count}")
    typer.echo(f"Snapshots:       {stats.snapshots_count}")
    typer.echo(f"Files written:   {len(stats.files_written)}")
    typer.echo(f"Output:          {stats.output_dir}")
    for f in stats.files_written:
        typer.echo(f"  - {f.relative_to(stats.output_dir)}")


dq_app = typer.Typer(
    name="dq",
    help="Data Quality engine — rules in meta.dq_rules; runs at injection points.",
    no_args_is_help=True,
)
app.add_typer(dq_app, name="dq")


@dq_app.command("list")
def dq_list(
    customer_repo: Path = typer.Option(
        Path.cwd(),
        "--customer-repo",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        help="Customer repo root (defaults to CWD).",
    ),
    env: str = typer.Option(..., "--env", help="Environment to read metadata for."),
    metadata_db: Path | None = typer.Option(
        None,
        "--metadata-db",
        help="SQLite metadata DB path. Defaults to "
        "<customer-repo>/.lake-ingest/metadata-<env>.sqlite.",
    ),
    dataset: str | None = typer.Option(None, "--dataset", help="Filter to one dataset (by name)."),
    layer: str | None = typer.Option(
        None, "--layer", help="Filter to one layer: bronze | silver | gold."
    ),
    include_disabled: bool = typer.Option(
        False, "--include-disabled", help="Show is_enabled=0 rules too (default: hide)."
    ),
) -> None:
    """List DQ rules currently in the metadata DB.

    Read-only — useful as a quick "did config sync land?" check, and
    as the DB-side counterpart to ``lake-ingest config sync``'s plan
    output.
    """
    from lake_ingestion_framework.core.dq import Layer
    from lake_ingestion_framework.core.metadata.repository import SqliteMetadataRepository

    db_path = metadata_db or _default_metadata_db(customer_repo, env)
    if not db_path.is_file():
        raise typer.BadParameter(
            f"Metadata DB not found at {db_path}. "
            f"Run `lake-ingest config sync --apply --env {env}` first."
        )
    layer_enum = Layer(layer) if layer else None

    repo = SqliteMetadataRepository(db_path)
    try:
        ds_filter_id: int | None = None
        if dataset:
            # Search by name across all sources — rules unique per (dataset_id, layer)
            for ds in repo.list_datasets():
                if ds.dataset_name == dataset:
                    ds_filter_id = ds.dataset_id
                    break
            if ds_filter_id is None:
                raise typer.BadParameter(f"Dataset {dataset!r} not found in metadata DB.")
        rules = repo.list_dq_rules(
            dataset_id=ds_filter_id,
            layer=layer_enum,
            only_enabled=not include_disabled,
        )
    finally:
        repo.close()

    if not rules:
        console.print("[yellow]No DQ rules found.[/yellow]")
        return

    console.print(f"[bold]Found {len(rules)} rule(s):[/bold]")
    for r in rules:
        status = "[red]disabled[/red] " if not r.is_enabled else ""
        qs = f", quarantine={r.quarantine_strategy.value}" if r.quarantine_strategy else ""
        console.print(
            f"  • [{r.rule_id}] {status}{r.layer.value}/dataset_id={r.dataset_id} "
            f"{r.rule_name}: {r.severity.value}/{r.action.value}{qs}"
        )
        console.print(f"      predicate: {r.predicate}")
        if r.description:
            console.print(f"      [dim]{r.description}[/dim]")


@dq_app.command("run")
def dq_run(
    customer_repo: Path = typer.Option(
        Path.cwd(),
        "--customer-repo",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
    ),
    env: str = typer.Option(..., "--env", help="Environment to read metadata for."),
    metadata_db: Path | None = typer.Option(None, "--metadata-db"),
    dataset: str = typer.Option(..., "--dataset", help="Dataset name (per dataset_master)."),
    layer: str = typer.Option(..., "--layer", help="bronze | silver | gold"),
    data_path: Path = typer.Option(
        ...,
        "--data-path",
        exists=True,
        readable=True,
        help="Delta directory or Parquet file to evaluate against.",
    ),
    injection_point: str = typer.Option(
        "post_processor",
        "--injection-point",
        help="post_reader | post_processor | post_write (default: post_processor).",
    ),
    run_id: str = typer.Option(
        "manual",
        "--run-id",
        help="Run id stamped on dq_results rows. Default: 'manual' for ad-hoc runs.",
    ),
) -> None:
    """Evaluate enabled DQ rules for one (dataset, layer, injection_point)
    against a Delta/Parquet path. Writes audit rows to ``meta.dq_results``
    + ``meta.dq_violations``; raises :class:`DqHaltError` for any
    ``stop_pipeline`` rule that fires.

    Useful for: smoke-checking newly synced rules; investigating
    ad-hoc data quality questions outside a pipeline run; standing up
    a DQ-only audit on a Delta table that some other tool produced.
    """
    from lake_ingestion_framework.core.dq import (
        ActionEngine,
        ActionTaken,
        DqHaltError,
        DuckdbEvaluator,
        InjectionPoint,
        Layer,
    )
    from lake_ingestion_framework.core.metadata.repository import (
        SqliteMetadataRepository,
    )

    db_path = metadata_db or _default_metadata_db(customer_repo, env)
    if not db_path.is_file():
        raise typer.BadParameter(
            f"Metadata DB not found at {db_path}. "
            f"Run `lake-ingest config sync --apply --env {env}` first."
        )

    layer_enum = Layer(layer)
    point_enum = InjectionPoint(injection_point)

    repo = SqliteMetadataRepository(db_path)
    try:
        ds = next((d for d in repo.list_datasets() if d.dataset_name == dataset), None)
        if ds is None:
            raise typer.BadParameter(f"Dataset {dataset!r} not found.")

        all_rules = repo.list_dq_rules(
            dataset_id=ds.dataset_id, layer=layer_enum, only_enabled=True
        )
        rules = [r for r in all_rules if r.injection_point == point_enum]
        if not rules:
            console.print(
                f"[yellow]No enabled rules at "
                f"({layer_enum.value}/{point_enum.value}) for dataset {dataset!r}.[/yellow]"
            )
            return

        # Lazy imports — duckdb + deltalake only needed for an actual run.
        import deltalake  # — gated on [local] extra
        import duckdb  # — gated on [dq] extra

        try:
            table = deltalake.DeltaTable(str(data_path)).to_pyarrow_table()
        except Exception as exc:
            raise typer.BadParameter(f"Cannot read Delta table at {data_path}: {exc}") from exc

        conn = duckdb.connect(":memory:")
        try:
            conn.register(dataset, table)
            evaluator = DuckdbEvaluator(conn)
            engine = ActionEngine(repo)

            counts = {"passed": 0, "failed": 0, "errored": 0}
            for row in rules:
                evaluation = evaluator.evaluate(_row_to_dqrule(row, dataset), relation_name=dataset)
                try:
                    outcome = engine.apply(
                        evaluation,
                        rule_id=row.rule_id,
                        run_id=run_id,
                        relation_name=dataset,
                    )
                except DqHaltError as halt:
                    console.print(
                        f"[red]HALT[/red] rule {halt.outcome.rule.name!r} "
                        f"({halt.outcome.violators_total} violator(s))"
                    )
                    raise typer.Exit(code=2) from halt

                if outcome.action_taken == ActionTaken.NONE:
                    counts["passed"] += 1
                    console.print(f"  [green]PASS[/green] {row.rule_name}")
                elif outcome.action_taken == ActionTaken.ERRORED:
                    counts["errored"] += 1
                    console.print(f"  [yellow]ERROR[/yellow] {row.rule_name}")
                else:
                    counts["failed"] += 1
                    console.print(
                        f"  [red]FAIL[/red] {row.rule_name} "
                        f"({outcome.violators_total} violator(s); "
                        f"action={outcome.action_taken.value})"
                    )

            console.print(
                f"\n[bold]Summary:[/bold] "
                f"{counts['passed']} passed, {counts['failed']} failed, "
                f"{counts['errored']} errored "
                f"(rules at {layer_enum.value}/{point_enum.value} "
                f"for dataset {dataset!r})."
            )
        finally:
            conn.close()
    finally:
        repo.close()


@dq_app.command("history")
def dq_history(
    customer_repo: Path = typer.Option(
        Path.cwd(),
        "--customer-repo",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
    ),
    env: str = typer.Option(..., "--env"),
    metadata_db: Path | None = typer.Option(None, "--metadata-db"),
    dataset: str | None = typer.Option(None, "--dataset"),
    rule_name: str | None = typer.Option(
        None, "--rule", help="Filter by rule name (combine with --dataset)."
    ),
    run_id: str | None = typer.Option(None, "--run-id"),
    limit: int = typer.Option(20, "--limit", help="Max rows to display."),
) -> None:
    """Show recent ``meta.dq_results`` rows.

    Output is a human-readable table — the audit timeline operators
    inspect after a pipeline run. For programmatic access, query
    ``meta.dq_results`` directly.
    """
    from lake_ingestion_framework.core.metadata.repository import (
        SqliteMetadataRepository,
    )

    db_path = metadata_db or _default_metadata_db(customer_repo, env)
    if not db_path.is_file():
        raise typer.BadParameter(f"Metadata DB not found at {db_path}.")

    repo = SqliteMetadataRepository(db_path)
    try:
        rule_id_filter: int | None = None
        if dataset and rule_name:
            ds = next((d for d in repo.list_datasets() if d.dataset_name == dataset), None)
            if ds is None:
                raise typer.BadParameter(f"Dataset {dataset!r} not found.")
            for layer_value in ("bronze", "silver", "gold"):
                from lake_ingestion_framework.core.dq import Layer

                for rule_row in repo.list_dq_rules(
                    dataset_id=ds.dataset_id, layer=Layer(layer_value), only_enabled=False
                ):
                    if rule_row.rule_name == rule_name:
                        rule_id_filter = rule_row.rule_id
                        break
                if rule_id_filter is not None:
                    break
            if rule_id_filter is None:
                raise typer.BadParameter(f"Rule {rule_name!r} not found for dataset {dataset!r}.")

        results = repo.list_dq_results(rule_id=rule_id_filter, run_id=run_id)
        # Most-recent first; truncate.
        results = list(reversed(results))[:limit]

        if not results:
            console.print("[yellow]No DQ results found for the given filters.[/yellow]")
            return

        from rich.table import Table

        table = Table(title=f"DQ history (most recent {len(results)})")
        table.add_column("result_id", justify="right")
        table.add_column("rule_id", justify="right")
        table.add_column("run_id")
        table.add_column("evaluated_at")
        table.add_column("status")
        table.add_column("violators")
        table.add_column("action")
        table.add_column("error", overflow="fold")

        for r in results:
            if r.error_message:
                status = "[yellow]errored[/yellow]"
            elif r.passed:
                status = "[green]passed[/green]"
            else:
                status = "[red]failed[/red]"
            table.add_row(
                str(r.result_id),
                str(r.rule_id),
                r.run_id,
                r.evaluated_at,
                status,
                str(r.violators_total),
                r.action_taken,
                r.error_message or "",
            )
        console.print(table)
    finally:
        repo.close()


def _row_to_dqrule(row: DqRuleRow, dataset_name: str) -> DqRule:
    """Tiny adapter to convert a ``DqRuleRow`` to a ``DqRule`` for the
    evaluator's natural-key surface. Lives in the CLI to keep the
    composition root self-contained; mirrors the helper inside
    :mod:`core.silver.pipeline`."""
    from lake_ingestion_framework.core.dq import DqRule

    return DqRule(
        name=row.rule_name,
        dataset_name=dataset_name,
        layer=row.layer,
        predicate=row.predicate,
        injection_point=row.injection_point,
        severity=row.severity,
        action=row.action,
        quarantine_strategy=row.quarantine_strategy,
        rule_type=row.rule_type,
        tech=row.tech,
        container=row.container,
        sample_cap=row.sample_cap,
        is_enabled=row.is_enabled,
        source=row.source,
        authored_by=row.authored_by,
        description=row.description,
    )


processors_app = typer.Typer(
    name="processors",
    help="Standard column processors (hygiene / type_safety / pii / dedup / ...).",
    no_args_is_help=True,
)
app.add_typer(processors_app, name="processors")


@processors_app.command("describe")
def processors_describe(
    customer_repo: Path = typer.Option(
        Path.cwd(),
        "--customer-repo",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
    ),
    env: str = typer.Option(..., "--env"),
    metadata_db: Path | None = typer.Option(None, "--metadata-db"),
    dataset: str = typer.Option(..., "--dataset"),
) -> None:
    """Show which processor groups + per-column transforms are active
    for a dataset. Read-only — useful for "what does my YAML actually
    do?" before running the pipeline.
    """
    from lake_ingestion_framework.core.metadata.repository import (
        SqliteMetadataRepository,
    )
    from lake_ingestion_framework.core.processors import (
        ProcessorEngine,
        ProcessorGroup,
    )

    db_path = metadata_db or _default_metadata_db(customer_repo, env)
    if not db_path.is_file():
        raise typer.BadParameter(f"Metadata DB not found at {db_path}.")

    repo = SqliteMetadataRepository(db_path)
    try:
        ds = next((d for d in repo.list_datasets() if d.dataset_name == dataset), None)
        if ds is None:
            raise typer.BadParameter(f"Dataset {dataset!r} not found.")

        engine = ProcessorEngine()
        config = engine.resolve_config(repo, dataset_id=ds.dataset_id)
        active_per_group = engine.describe(config)

        console.print(f"[bold]Dataset:[/bold] {dataset} (id={ds.dataset_id})")
        console.print(f"[bold]Active processor groups[/bold] (env: {env}):\n")
        for group in ProcessorGroup:
            active = active_per_group.get(group, [])
            if not active:
                console.print(f"  [dim]✗ {group.value:<22}[/dim] (disabled)")
                continue
            console.print(f"  [green]✓[/green] {group.value:<22} {len(active)} processors")
            for proc_name in active:
                console.print(f"      [dim]· {proc_name}[/dim]")
        console.print()

        if config.columns:
            console.print("[bold]Per-column transforms:[/bold]")
            for ctx in config.columns:
                rename = (
                    f" → {ctx.silver_column_name}"
                    if ctx.silver_column_name != ctx.bronze_column_name
                    else ""
                )
                cast = (
                    f" [cast: → {ctx.silver_data_type} ({ctx.cast_safety})]"
                    if ctx.silver_data_type
                    else ""
                )
                pii = (
                    f" [pii_class={ctx.pii_class}]"
                    if ctx.pii_class and ctx.pii_class != "none"
                    else ""
                )
                console.print(f"  · {ctx.bronze_column_name}{rename}{cast}{pii}")
                skips = config.column_skip_groups.get(ctx.bronze_column_name)
                if skips:
                    console.print(f"      [yellow]skip_groups:[/yellow] {sorted(skips)}")
                opt_ins = config.column_opt_in_processors.get(ctx.bronze_column_name)
                if opt_ins:
                    console.print(f"      [cyan]transforms:[/cyan] {sorted(opt_ins)}")
        else:
            console.print(
                "[yellow]No column metadata in dataset_config_columns; "
                "run `lake-ingest discover --apply` to populate.[/yellow]"
            )
    finally:
        repo.close()


# Cost Intelligence subcommands — delegated to costtool-core.
# `lake-ingest cost lint` / `cost inspect` are aliases of `costtool lint`
# / `costtool inspect`. The engine + CLI live in the standalone
# costtool-core package; install via the `[cost]` extra.
# See ADR 0009 for the open-core split rationale.
try:
    from costtool.cli import app as _costtool_app

    app.add_typer(
        _costtool_app,
        name="cost",
        help="Cost Intelligence — SQL lint + Delta physical-model inspection (via costtool-core).",
    )
except ImportError:
    # costtool-core isn't installed — register a stub that tells the
    # user to install the [cost] extra. Keeps `lake-ingest --help`
    # working without the extra installed.
    _cost_stub = typer.Typer(
        name="cost",
        help="Cost Intelligence — install with `pip install lake-ingest[cost]` to enable.",
        no_args_is_help=True,
    )

    @_cost_stub.callback(invoke_without_command=True)
    def _cost_not_installed(ctx: typer.Context) -> None:
        if ctx.invoked_subcommand is not None:
            return
        raise typer.BadParameter(
            "Cost Intelligence requires costtool-core. Install with: "
            "pip install lake-ingest[cost]"
        )

    app.add_typer(_cost_stub, name="cost")


lineage_app = typer.Typer(
    name="lineage",
    help="Lineage emission + queries (OpenLineage events).",
    no_args_is_help=True,
)
app.add_typer(lineage_app, name="lineage")


@lineage_app.command("emit-from-dbt")
def lineage_emit_from_dbt(
    manifest: Path = typer.Option(..., "--manifest", help="Path to dbt's target/manifest.json."),
    output: Path = typer.Option(..., "--output", help="JSONL output file — one OL event per line."),
    namespace: str = typer.Option(
        "delta://lakehouse",
        "--namespace",
        help="OL dataset namespace (e.g., 'databricks://workspace.cloud.databricks.com').",
    ),
    job_namespace: str = typer.Option(
        "dbt",
        "--job-namespace",
        help="OL job namespace; groups all dbt-emitted events.",
    ),
) -> None:
    """Emit OpenLineage events from a dbt manifest.

    Reads ``target/manifest.json``, walks every Gold model + snapshot,
    and writes one OL ``COMPLETE`` event per output dataset to a JSONL
    file. Output is byte-identical for the same manifest (deterministic
    run-IDs + manifest-derived event times).
    """
    from lake_ingestion_framework.core.lineage import (
        LineageEmitConfig,
        emit_from_dbt_manifest,
    )

    stats = emit_from_dbt_manifest(
        LineageEmitConfig(
            manifest_path=manifest,
            output_path=output,
            namespace=namespace,
            job_namespace=job_namespace,
        )
    )

    typer.echo(f"Events written:  {stats.events_written}")
    typer.echo(f"Distinct inputs: {stats.inputs_count}")
    typer.echo(f"Distinct outputs:{stats.outputs_count}")
    if stats.skipped_nodes:
        typer.echo(f"Skipped nodes:   {len(stats.skipped_nodes)} (tests/seeds/etc.)")
    typer.echo(f"Output:          {stats.output_path}")
    typer.echo(f"Duration:        {stats.duration_seconds:.3f}s")


# ── Query commands (operate on emitted JSONL files) ──────────────────


def _resolve_dataset_or_exit(graph: LineageGraph, term: str) -> str:
    """Look up a dataset by name; exit cleanly if 0 or many matches."""
    matches = graph.find(term)
    if not matches:
        typer.echo(f"No dataset matched {term!r}.", err=True)
        raise typer.Exit(code=2)
    if len(matches) > 1:
        typer.echo(
            f"{term!r} matched {len(matches)} datasets — disambiguate with the "
            "qualified form (namespace/name):",
            err=True,
        )
        for n in matches:
            typer.echo(f"  • {n.qualified}", err=True)
        raise typer.Exit(code=2)
    return matches[0].qualified


def _print_query_result(
    *,
    title: str,
    nodes: set[str],
    fmt: str,
) -> None:
    if fmt == "json":
        import json as _json

        typer.echo(_json.dumps({"datasets": sorted(nodes)}, indent=2))
        return

    typer.echo(title)
    if not nodes:
        typer.echo("  (none)")
        return
    for n in sorted(nodes):
        typer.echo(f"  • {n}")


def _load_graph(files: list[Path]) -> LineageGraph:
    from lake_ingestion_framework.core.lineage import LineageGraph

    return LineageGraph.from_jsonl(files)


_FORMAT_HELP = "Output format: 'text' (default, human-readable) | 'json' (for tooling)."


@lineage_app.command("upstream")
def lineage_upstream(
    dataset: str = typer.Argument(..., help="Dataset name (or namespace/name)."),
    files: list[Path] = typer.Option(
        [], "--lineage-file", help="JSONL file(s) emitted by lineage emitters (repeatable)."
    ),
    depth: int = typer.Option(-1, "--depth", help="Max upstream hops (-1 = unlimited)."),
    fmt: str = typer.Option("text", "--format", help=_FORMAT_HELP),
) -> None:
    """Show datasets that flow *into* this one."""
    if not files:
        raise typer.BadParameter("Pass at least one --lineage-file.")
    graph = _load_graph(files)
    qualified = _resolve_dataset_or_exit(graph, dataset)
    upstream = graph.upstream(qualified, max_depth=depth)
    _print_query_result(
        title=f"Upstream of {qualified} (depth={depth if depth >= 0 else 'unlimited'}):",
        nodes=upstream,
        fmt=fmt,
    )


@lineage_app.command("downstream")
def lineage_downstream(
    dataset: str = typer.Argument(..., help="Dataset name (or namespace/name)."),
    files: list[Path] = typer.Option(
        [], "--lineage-file", help="JSONL file(s) emitted by lineage emitters (repeatable)."
    ),
    depth: int = typer.Option(-1, "--depth", help="Max downstream hops (-1 = unlimited)."),
    fmt: str = typer.Option("text", "--format", help=_FORMAT_HELP),
) -> None:
    """Show datasets this one flows *into*."""
    if not files:
        raise typer.BadParameter("Pass at least one --lineage-file.")
    graph = _load_graph(files)
    qualified = _resolve_dataset_or_exit(graph, dataset)
    downstream = graph.downstream(qualified, max_depth=depth)
    _print_query_result(
        title=f"Downstream of {qualified} (depth={depth if depth >= 0 else 'unlimited'}):",
        nodes=downstream,
        fmt=fmt,
    )


@lineage_app.command("blast-radius")
def lineage_blast_radius(
    dataset: str = typer.Argument(..., help="Dataset name (or namespace/name)."),
    files: list[Path] = typer.Option(
        [], "--lineage-file", help="JSONL file(s) emitted by lineage emitters (repeatable)."
    ),
    fmt: str = typer.Option("text", "--format", help=_FORMAT_HELP),
) -> None:
    """Show everything potentially affected by a change to this dataset.

    Same data as ``downstream`` but framed for impact analysis: the
    answer to 'what breaks if I change X?'. Phase 15 will overlay
    severity weights.
    """
    if not files:
        raise typer.BadParameter("Pass at least one --lineage-file.")
    graph = _load_graph(files)
    qualified = _resolve_dataset_or_exit(graph, dataset)
    radius = graph.blast_radius(qualified)
    _print_query_result(
        title=f"Blast radius of {qualified} ({len(radius)} datasets affected):",
        nodes=radius,
        fmt=fmt,
    )


@lineage_app.command("path")
def lineage_path(
    src: str = typer.Argument(..., help="Source dataset (name or namespace/name)."),
    dst: str = typer.Argument(..., help="Destination dataset (name or namespace/name)."),
    files: list[Path] = typer.Option(
        [], "--lineage-file", help="JSONL file(s) emitted by lineage emitters (repeatable)."
    ),
    fmt: str = typer.Option("text", "--format", help=_FORMAT_HELP),
) -> None:
    """Show the shortest forward path from ``src`` to ``dst``, if any."""
    if not files:
        raise typer.BadParameter("Pass at least one --lineage-file.")
    graph = _load_graph(files)
    src_q = _resolve_dataset_or_exit(graph, src)
    dst_q = _resolve_dataset_or_exit(graph, dst)
    path = graph.path(src_q, dst_q)

    if fmt == "json":
        import json as _json

        typer.echo(_json.dumps({"path": path}, indent=2))
        return

    if path is None:
        typer.echo(f"No path from {src_q} to {dst_q}.")
        return

    typer.echo(f"Path ({len(path) - 1} hop{'s' if len(path) - 1 != 1 else ''}):")
    for i, node in enumerate(path):
        prefix = "  " if i == 0 else "  ↓\n  "
        typer.echo(f"{prefix}{node}")


@app.command()
def demo(
    no_gold: bool = typer.Option(
        False,
        "--no-gold",
        help="Skip the Gold/dbt leg (auto-skipped on Python 3.14 anyway).",
    ),
    keep_postgres: bool = typer.Option(
        False,
        "--keep-postgres",
        help="Don't tear down the postgres container at the end.",
    ),
    workspace: Path = typer.Option(
        Path("/tmp/lake-ingest-demo"),  # noqa: S108 — demo default; user can override
        "--workspace",
        help="Where to write Bronze / Silver / Gold artifacts. Wiped at start.",
    ),
) -> None:
    """End-to-end local demo: postgres → Bronze → Silver → Gold + lineage.

    Boots Docker postgres, ingests two tables through every owned layer,
    emits OpenLineage events at every hop, and prints lineage queries.
    See ``scripts/demo_end_to_end.py`` for what each stage does.

    Pre-reqs: Docker Desktop running + ``[postgres]`` + ``[local]`` +
    ``[lineage]`` extras (and ``[gold_dev]`` + Python 3.11-3.13 for the
    Gold leg). Equivalent to ``python scripts/demo_end_to_end.py`` —
    this command is just for discoverability.
    """
    import sys as _sys

    script = _framework_root() / "scripts" / "demo_end_to_end.py"
    if not script.is_file():
        raise typer.BadParameter(
            f"demo script not found at {script}. Are you running from the framework's source tree?"
        )

    args = [_sys.executable, str(script), "--workspace", str(workspace)]
    if no_gold:
        args.append("--no-gold")
    if keep_postgres:
        args.append("--keep-postgres")

    # Replace this process — the demo writes to stdout in real time and
    # there's nothing for the CLI to do after it returns. args[0] is the
    # current Python interpreter, args[1] is a path inside the framework
    # tree; nothing is user-controlled.
    os.execv(args[0], args)  # noqa: S606 — args fully constructed above


@app.command(name="reset-watermark")
def reset_watermark(
    dataset: str = typer.Option(..., "--dataset", help="Dataset name."),
    to: str = typer.Option(..., "--to", help="New watermark value."),
    reason: str = typer.Option(..., "--reason", help="Audit reason (required, non-empty)."),
    confirm: bool = typer.Option(False, "--confirm", help="Required for any actual change."),
    delete_affected_partitions: bool = typer.Option(
        False, "--delete-affected-partitions", help="Also delete partitions newer than --to."
    ),
    apply: bool = typer.Option(False, "--apply", help="Actually run (default: dry-run)."),
) -> None:
    """Audited rollback of a watermark.

    See ``docs/recovery.md`` → "reset-watermark".
    """
    raise NotImplementedError(
        "lake-ingest reset-watermark: implementation coming in the next phase. "
        "See docs/recovery.md for the design."
    )


# ──────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────


def main() -> None:
    """Console-script entry point. Wired in pyproject.toml."""
    app()


if __name__ == "__main__":
    main()
