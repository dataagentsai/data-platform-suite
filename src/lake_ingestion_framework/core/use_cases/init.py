"""``lake-ingest init`` — scaffold a customer repo from a profile.

Reads a ``customer-profile.yaml``, renders the
``templates/customer_repo_template/`` tree filtered to what the profile
declares, writes the result to a target directory.

Dry-run by default: returns the planned file list without writing.
``--apply`` is explicit.

See ``docs/customer_profile.md`` and ``docs/local_install.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape

from lake_ingestion_framework._version import __version__
from lake_ingestion_framework.core.exceptions import ValidationError
from lake_ingestion_framework.core.profile import CustomerProfile, load_profile


@dataclass(frozen=True)
class PlannedFile:
    """One file the init command will write."""

    target_path: Path
    template_name: str
    rendered: str


@dataclass(frozen=True)
class InitPlan:
    """Result of resolving a profile against the template tree."""

    profile: CustomerProfile
    target_dir: Path
    files: tuple[PlannedFile, ...]


# ──────────────────────────────────────────────────────────────────────
# Public entry points
# ──────────────────────────────────────────────────────────────────────


def plan_init(
    *,
    profile_path: Path,
    target_dir: Path | None,
    template_root: Path,
    framework_local_path: Path,
) -> InitPlan:
    """Resolve a profile + template tree into a list of files to write.

    Pure (no side effects). Tests assert the file list and contents
    against fixtures.
    """
    profile = load_profile(profile_path)

    if target_dir is None:
        target_dir = Path.cwd() / f"{profile.customer.name}-ingestion"

    profile_yaml = _read_profile_yaml(profile_path)
    pip_extras = _resolve_pip_extras(profile)
    requires_shir = any(s.requires_shir for s in profile.sources)

    env = Environment(
        loader=FileSystemLoader(str(template_root)),
        undefined=StrictUndefined,
        # We emit YAML / Markdown / TOML; HTML/XML templates would be
        # auto-escaped if present, but we don't have any. Explicit
        # select_autoescape narrows the bandit/semgrep B701 surface.
        autoescape=select_autoescape(enabled_extensions=("html", "htm", "xml")),
        keep_trailing_newline=True,
    )

    files: list[PlannedFile] = []

    # ── Top-level files
    for template_name, target_name in (
        ("README.md.j2", "README.md"),
        ("pyproject.toml.j2", "pyproject.toml"),
        (".gitignore.j2", ".gitignore"),
        ("customer-profile.yaml.j2", "customer-profile.yaml"),
    ):
        rendered = env.get_template(template_name).render(
            customer=profile.customer,
            environments=profile.environments,
            sources=profile.sources,
            features=profile.features,
            pip_extras=pip_extras,
            framework_local_path=str(framework_local_path),
            framework_version=__version__,
            profile_yaml=profile_yaml,
            requires_shir=requires_shir,
        )
        files.append(
            PlannedFile(
                target_path=target_dir / target_name,
                template_name=template_name,
                rendered=rendered,
            )
        )

    # ── Per-environment platform_config
    env_template = env.get_template("config/platform_config/_env.yaml.j2")
    for environment in profile.environments:
        rendered = env_template.render(
            customer=profile.customer,
            env=environment,
            features=profile.features,
            requires_shir=requires_shir,
        )
        files.append(
            PlannedFile(
                target_path=(
                    target_dir / "config" / "platform_config" / f"{environment.name}.yaml"
                ),
                template_name="config/platform_config/_env.yaml.j2",
                rendered=rendered,
            )
        )

    return InitPlan(profile=profile, target_dir=target_dir, files=tuple(files))


def apply_init(plan: InitPlan, *, overwrite: bool = False) -> None:
    """Write every file in ``plan.files`` to disk.

    Refuses to overwrite existing files unless ``overwrite=True``.
    Creates intermediate directories as needed.
    """
    if plan.target_dir.exists() and any(plan.target_dir.iterdir()) and not overwrite:
        raise ValidationError(
            f"Target directory already non-empty: {plan.target_dir}. "
            "Pass overwrite=True (CLI: --update) to write into it."
        )

    plan.target_dir.mkdir(parents=True, exist_ok=True)
    for pf in plan.files:
        pf.target_path.parent.mkdir(parents=True, exist_ok=True)
        if pf.target_path.exists() and not overwrite:
            raise ValidationError(f"File already exists: {pf.target_path}")
        pf.target_path.write_text(pf.rendered, encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _read_profile_yaml(path: Path) -> str:
    """Return the profile YAML as a string for verbatim echo into the
    customer repo's ``customer-profile.yaml``.

    Round-tripping through Pydantic + yaml.dump would lose comments;
    keeping the raw text preserves them.
    """
    return path.read_text(encoding="utf-8").rstrip() + "\n"


def _resolve_pip_extras(profile: CustomerProfile) -> str:
    """Return a comma-separated extras list for the customer's pyproject.toml.

    Always includes the realm extra (``adf``) plus one extra per
    declared source type. Order is alphabetical for deterministic
    golden-diff output.
    """
    extras: set[str] = {"adf"}
    for source in profile.sources:
        extras.add(source.source_type.value)
    return ",".join(sorted(extras))


def profile_summary(profile: CustomerProfile) -> dict[str, object]:
    """Compact dict view of a profile — for CLI output + test assertions."""
    return {
        "customer": profile.customer.name,
        "environments": [e.name for e in profile.environments],
        "source_types": sorted({s.source_type.value for s in profile.sources}),
        "features_on": sorted(k for k, v in profile.features.model_dump().items() if v is True),
    }


__all__ = [
    "InitPlan",
    "PlannedFile",
    "apply_init",
    "plan_init",
    "profile_summary",
]
