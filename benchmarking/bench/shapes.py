"""Write a copy of a catalog slice whose world file carries a worst-case data shape (a `stress:` block). Pure file work:
it generates no data. The engine job then runs the copy on the workspace.

    python -m bench.shapes --profile worst_combined --src ~/data-platform-catalog/slices/02-tyre \
        --dst _slices/02-tyre-worst_combined
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
from pathlib import Path
from typing import Any

import yaml

HERE = Path(__file__).resolve().parent.parent
_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


def profiles() -> dict[str, dict[str, Any]]:
    return yaml.safe_load((HERE / "scenarios/shapes.yaml").read_text())["profiles"]


def known_settings() -> set[str]:
    """The stress settings the tyre world understands, read from the world code so the two cannot drift."""
    from dataplatform.world.domains.tyre import STRESS_DEFAULTS  # noqa: PLC0415 -- needs src on the path

    return set(STRESS_DEFAULTS)


def stress_block(profile: dict[str, Any], known: set[str]) -> str:
    bad = sorted(set(profile) - known)
    if bad:
        raise ValueError(f"unknown stress setting {bad}; the world knows {sorted(known)}")
    if not profile:
        return ""
    lines = ["", "stress:                       # worst-case shape (benchmarking/scenarios/shapes.yaml)"]
    for k in sorted(profile):
        if not _NAME.match(k) or isinstance(profile[k], bool) or not isinstance(profile[k], (int, float)):
            raise ValueError(f"stress setting {k} must be a number")
        lines.append(f"  {k}: {profile[k]}")
    return "\n".join(lines) + "\n"


def write_slice(src: Path, dst: Path, profile: dict[str, Any], known: set[str]) -> Path:
    """Copy `src` to `dst` and append the profile to its world file. Refuses a world that already has a stress block."""
    world = src / "world.yaml"
    if not world.exists():
        raise FileNotFoundError(f"{src} has no world.yaml")
    text = world.read_text()
    if re.search(r"^stress:", text, flags=re.M):
        raise ValueError(f"{world} already has a stress block")
    block = stress_block(profile, known)
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)
    (dst / "world.yaml").write_text(text.rstrip("\n") + "\n" + block)
    return dst


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--profile", required=True, choices=sorted(profiles()))
    ap.add_argument("--src", type=Path, required=True)
    ap.add_argument("--dst", type=Path, required=True)
    a = ap.parse_args(argv)
    sys.path.insert(0, str(HERE.parent / "src"))
    out = write_slice(a.src.expanduser(), a.dst, profiles()[a.profile], known_settings())
    print(f"{a.profile} -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
