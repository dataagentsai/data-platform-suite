"""End-to-end smoke tests.

These tests exercise the *full* local-mode loop: a real source DB
container (postgres for the headline test), the framework's connector,
the ADF Mimic, and the LocalDeltaWriter writing real Delta files.

Marked ``pytest.mark.smoke`` so they're opt-in via ``pytest -m smoke``.
Default test runs skip them — they take longer and require Docker.
"""
