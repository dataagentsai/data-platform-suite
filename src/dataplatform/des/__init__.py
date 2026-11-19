"""Data expectation services (DES, ADR-024 set c): whether the data is right. Quality results, reconciliation,
profiling, quarantine and release. Owned here, filled by session W1e.

DES judges its own checks and writes one step row per check in the one step format (obs/steps.py: `quality.*`,
`recon.*`, `profile.*` with a severity). Observability reads only that row to alert and to mark a run; the detail,
the screens and the fixes (quarantine release goes through the release flow) stay here.

    dashboards.py   the DES screens as product-free definitions (generated beside observability's, owned by DES)
"""
