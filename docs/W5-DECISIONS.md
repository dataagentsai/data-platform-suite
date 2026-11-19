---
title: "W5 semantic layer, BI, governance and access (cost moved to the separate cost tool): what was built before gold, and what the owners decide"
audience: [architect, engineer, analyst, finops]
purpose: "Decisions taken while building the W5 generators (no live data), and the open questions for the BI owners and the governance owner"
status: draft
last-reviewed: 2026-11-04
related:
  - semantic_layer_design.md
  - security.md
  - cost.md
---

# W5 before gold: decisions and open questions

Branch `w5-semantic-governance`, built on W1a (7dbcd8c) while gold (W1d) does not exist yet. Everything here is a
generator from the tyre slice's manifest and contracts; nothing needs live data. Specs: ADR-012, ADR-022, DPC-0062,
DPC-0066, DPC-0070, DPC-0073, DPC-0075, DPC-0076; DAC SERV-04, SERV-06, SEC-01, SEC-02, ENV-02, CAT-01, COST-03.

## What exists

| Part | Kernel (product-free, R1) | Databricks realm | Output (golden files) |
|---|---|---|---|
| Semantic layer | `dataplatform/semantic/metrics.py`: metric files read, linted (names, lifecycle, known answers, one word one metric), split into certified view and preview | `render.metric_view_sql`, `render.known_answers_sql` | `generated/tyre/<env>/semantic/mv_*.sql`, `known_answers.sql` |
| BI | `dataplatform/semantic/experiences.py`: dashboard and conversation-space definitions | `render.dashboard_json`, `render.genie_space_json` | `generated/tyre/<env>/bi/*.lvdash.json`, `genie_space.json` |
| Governance | `dataplatform/governance/`: tables from contracts, tag standard, data products and shares, desired entries, `plan_sync` diff; `CatalogState` port with memory and Unity Catalog adapters | `render.catalog_sync_sql`, `render.audit_sql` | `generated/tyre/<env>/governance/catalog_sync.sql`, `audit.sql`, `generated/tyre/findings.md` |
| Access | `dataplatform/access/model.py`: groups and classifications to grants, masks, row filters; `visible_columns` / `row_visible` (the port's check) | `render.access_sql` | `generated/tyre/<env>/access/access.sql` |
| Job tags (cost is the separate cost tool's since 4 Nov 2026) | `governance/tables.py`: `COMPUTE_TAGS`, `compute_tags()` | none | none (the cost queries and their generator were removed in W1d) |

Regenerate: `uv run --no-project --with pyyaml python realms/databricks/semantic_governance/generate.py`
(`--check` fails on a stale file). The access model input is `realms/databricks/semantic_governance/access/tyre.yaml`.

## Decisions taken (change any of them by editing the generator, not the output)

1. **Two views per metric file when needed.** The manifest's name (`mv_otif`, `mv_sales`, `mv_scrap`) holds
   certified measures only; it is what dashboards and the Genie space read (SERV-06 enforced by what is attached, not
   only by instructions). Measures still draft or reviewed go to `<name>_preview` (with the certified ones, so owners
   compare like for like), tagged with the lowest state. Deprecated measures go nowhere. `mv_otif` has no preview.
2. **Joins come from the gold fact's manifest** (key and schema), declared `cardinality: many_to_one` (surrogate keys
   are unique per version, ADR-004). The cardinality field needs metric-view spec 1.1 on Runtime 18.1 or later.
3. **Known answers are parsed into dimension filters** (`'text'`, `YYYY-MM`, `YYYY-MM-DD`), so the same address is
   asked of the metric view (`known_answers.sql`) and of the world oracle. Checked now: every address resolves in a
   small world run, and at full scale (`DP_WORLD_FULL=1`, 3.5 minutes) the oracle equals every manifest known answer.
4. **Names follow ADR-012**: catalogs `<env>_<layer>`, silver schemas per source and gold per domain, read from the
   contract's `physicalName` (`silver_erp.delivery`, `gold_sales.fact_otif`). Files are generated per environment in
   `platform.yaml` (dev, prod) so each SQL file runs as is.
5. **Tag standard** (kernel `MANAGED_TAGS`): tables `domain, layer, data_product, used_by, owner, contract,
   certification, glossary_terms`; columns `classification, protection`. Multi-value tags use ` | `, never commas.
   The sync changes and removes only these keys; anyone else's tags are left alone.
6. **Owner is a tag, not the UC owner.** Tables stay owned by the deploying service principal; the contract's owner
   mailbox goes in the `owner` tag. Changing UC ownership to a team group would stop jobs from managing their tables.
7. **Contract wins on owner conflicts, and the conflict is reported** in `findings.md`, never silently resolved.
8. **Unclassified columns are `internal`** (SEC-01 wants every column classified; the contracts mark only restricted).
9. **Restricted means tokenised.** Every `restricted` column is personal data tokenised before bronze (DPC-0070,
   PII-01; both restricted fields in the slice say so). Groups allowed to see it get the token; nothing detokenises.
   `confidential` columns would be masked but not tokenised.
10. **Grants in UC SQL from the realm**, not Terraform (the binding names Terraform). SQL runs on Free Edition today
    and is a smaller change; the plan is product-free, so a Terraform renderer is a second realm function later.
11. **Grants are planned, not diffed yet.** `access.sql` grants what the model says; it does not revoke grants made by
    hand. A grant-state port (like `CatalogState`) is the next step for CAT-01-style drift on access.
12. **Mask functions live in `<env>_ops.access`**, one per masked column, named `mask_<layer>_<schema>_<table>_<col>`;
    text masks to `***`, other types to NULL. Parameter types come from the contract's logical type.
13. **Data product = a gold fact with its metric views** (`otif`, `sales`, `scrap`). Silver datasets and dimensions
    are shared inputs, split across the products that read them in equal shares; a dataset nobody reads (press
    readings today) stays `(unattributed)` instead of being spread.
14. **Jobs carry the tag convention** (`COMPUTE_TAGS`: env, domain, data_product, source, dataset, cost_center).
    Usage rows carry custom tags only on classic compute or with serverless budget policies (not on Free Edition), so
    a cost reader falls back to the job's own tags (`system.lakeflow.jobs.tags`).
15. **Cost belongs to the separate cost tool (decision of 4 Nov 2026).** The generated `cost/cost_per_data_product.sql` and
    `cost/untagged_spend.sql`, their renderers and the equal-shares attribution (`shares()`) were removed in W1d; the
    platform keeps only the tag convention, which the separate cost tool reads.
16. **Glossary = certified measure names with synonyms**, as `glossary_terms` tags and the views' comments. Unity
    Catalog has no glossary object; a governed-tag list or an external glossary is the governance owner's call (below).

## For the BI owners

- **Dashboard layout.** Generated: one dashboard per domain, a page per certified view, and per certified measure a
  headline number, a trend over its time dimension and a bar per other dimension. Do you want a layout file in the repo
  (which tiles, order, filters), or is the derived layout enough for the demo?
- **Preview measures on dashboards.** Today `Net sales value` (draft) and `Tyres cured` (reviewed) appear on no
  dashboard. Should a clearly labelled preview page show them to metric owners, or do they stay in `_preview` views
  for SQL only?
- **Sample questions.** Generated from the known answers ("What was OTIF % for region south in 2026-11?") and the
  first synonym ("Show on-time delivery by region for each promised month."). Propose adding `sample_questions:` to
  the metric files so the demo stories' own wording is used.
- **Genie instructions.** Certified views only, show the query, business time zone, synonyms, and decline the two
  non-certified measures by name. Anything to add for the demo (e.g. default time window)?
- **Genie space and dashboard file shapes** follow the public `serialized_space` and `.lvdash.json` shapes; check them
  on the first deploy (the deployer adds the SQL warehouse id).

## For the governance owner

- **Access model.** `access/tyre.yaml`: `data-platform-jobs` (service principals, all layers, see tokens),
  `platform-engineers` (bronze to gold, protected columns masked), the three contract teams (silver and gold of their
  domain, see tokens), `analysts` (gold only, ADR-012). Are these the groups? Should engineers read bronze, which
  holds tokens unmasked (bronze has no contract, so no column masks)?
- **Row filters.** The mechanism is built and tested (deny by default), but no spec declares one. Is there a regional
  rule for the demo (SEC-02's failure mode)? Facts carry `dealer_sk`, not region, so a region filter needs a
  dimension-driven rule or a region column on the facts.
- **Owner mismatch found:** `tyre-lims-inspection-result` contract team `quality-data` lists
  `manufacturing-data@tyre.example` as owner; the manifest says `quality-data@tyre.example`. Which is right?
- **Classification vocabulary.** We use `public, internal, confidential, restricted` and treat `restricted` as
  personal data. Do you want a separate personal-data flag (ODCS `customProperties`) so non-personal restricted data
  is possible?
- **Glossary home.** Tags and comments in Unity Catalog, or a glossary tool fed from the same metric files?
- **Data products.** Agree "a gold fact with its metric views" (how shared inputs are split for cost is now
  the separate cost tool's question, not the platform's).
- **Home of the access model.** Proposed: move `access/tyre.yaml` into the catalog slice as a DPM `access_model`
  file, so the framework repo holds no domain data.

## Known answers on gold (W1d)

`make run` now builds gold and asks every certified known answer of it the way the metric view defines it
(`dataplatform/semantic/answers.py`: the measure expression over the fact joined to its dimensions, filtered on the
view's dimensions, in the business time zone), writing `build/run/gold_metrics.json`. `make gold-known-answers DAYS=14`
runs `test_metric_view_matches_known_answer_pending_gold` and `::test_metric_view_matches_world_oracle_pending_gold`
with `DP_GOLD_METRICS` and `DP_GOLD_ORACLE` set to that run. Results and the one spec gap they found (`OTIF %` counts
promised dates not yet due) are in `docs/W1D-GOLD.md`. Still waiting on a workspace: running the generated SQL, the
Genie space and dashboards, lineage source to gold to dashboard, and the masked-column check as a non-privileged user.
