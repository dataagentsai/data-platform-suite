"""Single source of truth for the package version.

Read by ``lake_ingestion_framework.__init__`` and surfaced via
``lake-ingest --version``. Bumped per SemVer rules (see ``docs/repo_structure.md``).
"""

__version__ = "0.1.0.dev0"
