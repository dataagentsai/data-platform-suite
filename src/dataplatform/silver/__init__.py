"""Silver (W1c): the thirteen standard stages from bronze to silver (ADR-007, -008, -013, -016), product-free.

    plan.py     what each stage does for one dataset, from the metadata store's merged view
    ports.py    stage_runner and the silver read and write edges
    runner.py   one silver run: new bronze batches -> stages -> silver, every step in the run log
    daybyday.py silver per world day, compared with the oracle's silver totals; incidents judged

The stages themselves run on a dataframe engine behind the stage_runner port (dataplatform.stages).
"""
