"""Metadata-DB layer: key registry, models, repository.

Python's interactions with the metadata DB are scoped to deploy /
discover / register / ops commands. The runtime path is ADF + stored
procs; Python doesn't model log-table writes — only reads. See
``docs/repo_structure.md`` and ADR 0004.
"""
