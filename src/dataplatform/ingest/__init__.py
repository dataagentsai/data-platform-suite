"""Ingestion (W1b; ADR-002, D11): sources -> bronze, driven by the metadata store, every run logged in it.

    ports.py      the ports ingestion talks to: landing (push), the bronze writer, the tokenizer
    steps.py      the lander steps, one library for every way in (ING-01): parse, contract, derive, protect,
                  volume, mass-delete guard, stamp, write, log
    pipelines.py  one pipeline per source type, chosen by the dataset's ingest mode and read:
                      push   change_feed, files      landing -> lander
                      pull   full_copy, pages        the lander steps in-process, straight to bronze
                      stream events                  the stream bus is the landing; trigger-once, from checkpoints
    runner.py     one run: start_run -> per dataset start_dataset_run ... finish_dataset_run -> finish_run,
                  for run types normal, reload and backfill

Names here say what a thing is, never which product holds it (R1).
"""
