"""DataTwin (goal G10): the tyre world's incidents as end-to-end scenarios, and the release gate.

    scenario.py    the scenario file (dwd-scenario/v0, AgentTwin's shape for a data world): parse and validate
    scenarios/     one file per planted incident; learned/ holds scenarios made from production failures
    runner.py      the harness (world + platform run log) and the oracle checks; pending checks name their layer
    learning.py    the learning loop: a failed run becomes a pending scenario file
    gate.py        the release gate: clone prod, run the change, compare with the oracle and prod, pass or block
"""
