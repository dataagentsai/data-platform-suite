# Benchmark requirements (from the user, 23 Nov 2026)

Kept as given, then read back in plain words. Change this file first when a requirement changes; the plan follows it.

## What the user asked for

1. **A worst-case scenario benchmark, start to end, ending in Power BI.** Power BI is the important end: the worst
   cases are judged by what a report user sees (time to the first visual, a refresh that finishes, numbers that are
   right), not only by what the platform does inside.
2. **A synthetic data generator built from that point of view.** The data is shaped to hurt: it is built to make the
   slowest, most expensive and most wrong-prone version of each stage.
3. **Do not generate data here. The mechanics are built here; the data is generated in the office Databricks.**
   Nothing in this folder may need a large machine, a cloud account or a Power BI service to be written and tested.
   Everything that does is a run step with a written procedure and a result file the user brings back.
4. **All three table formats, every layer.** Databricks managed Iceberg, Databricks managed Delta with UniForm, and AWS
   S3 Tables; bronze to silver, silver to gold, gold to semantic. Runs of 3 days are fine.
5. **All cross-cutting concerns (X1 to X22) are checked**, including data quality, profiling, reconciliation,
   observability and governance. Cost is read from the separate cost tool through the tag standard.
6. **Table maintenance must be benchmarked, not assumed.** "With all three formats, how would you do table maintenance,
   and is all of Databricks' table maintenance supported?" The user wants to check that. Each maintenance claim in
   `scenarios/maintenance.yaml` is marked *unverified* until a benchmark case has run it.
7. **The benchmark exists to test the key differentiators already analysed.** If a benchmark is an easier way to settle
   a differentiator than more analysis, run the benchmark.

## In plain words

- We build the generator, the scenario definitions, the runner and the result format here, tested on a laptop with a
  tiny scale.
- The user runs them in the office Databricks and Power BI at full scale and brings the result files back.
- The answer to "which format, and what maintenance" comes from measured results, not from the docs.

## What is not decided (assumed until the user says otherwise)

| Item | Assumption | Why |
|---|---|---|
| "Managed AWS S3" | AWS S3 Tables (A14 in clean-data-engineering/TODO.md) | The AWS-managed Iceberg service. |
| Power BI connection modes | Import, DirectQuery and a composite model are all in scope; Direct Lake is optional (needs Fabric) | The worst case differs by mode. |
| Scale | 3-day compressed world as the base run, with a scale multiplier up to the largest the office workspace allows | The user said 3 days is fine. |
| Data world | The tyre world, with worst-case shapers added (reuse first) | It already has sources, incidents and an oracle. |
