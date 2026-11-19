"""Contract tests for the metadata store port (ADR-010, M5): every adapter passes the same table of cases.

Each row is (case, act, expect): `act` runs against a store seeded with a small domain; `expect` is the value it must
return, or the refusal it must raise. The PostgreSQL rows run when a local server answers (DP_META_TEST_DSN, default
dbname=dp_meta_test; the database is created if missing) and are skipped otherwise. Each PostgreSQL row starts from a
freshly installed schema, so every row also proves the procedures one at a time.

Rules proven here (DAC): CFG-05 (an unknown setting is refused), CFG-04 (the store is changed only through its
procedures).
"""

import os
import re
from dataclasses import replace
from pathlib import Path

import pytest

from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.meta.port import (
    ERROR_CAP,
    BackfillPlan,
    Correction,
    DqResult,
    DqRule,
    FileRecord,
    InvalidArgument,
    NotFound,
    RunStateError,
    SettingDef,
    StepRecord,
    UnknownSetting,
)

DEFS = [
    SettingDef("load_type", "dataset", "enum", None, ("FULL", "INCREMENTAL")),
    SettingDef("retry_count", "dataset", "int", "2"),
    SettingDef("cost_center", "dataset", "string"),
    SettingDef("schedule_timezone", "dataset", "string", "UTC"),
    SettingDef("default_realm", "platform", "enum", None, ("local",)),
]
COLUMNS = [
    {"name": "order_id", "logical_type": "string", "required": True, "primary_key_position": 1, "description": "",
     "classification": ""},
    {"name": "line_no", "logical_type": "integer", "required": True, "primary_key_position": 2, "description": "",
     "classification": ""},
    {"name": "net_value", "logical_type": "number", "required": False, "primary_key_position": None,
     "description": "value in the order currency", "classification": "restricted"},
]
DOMAIN = {"domain": "shop", "business_timezone": "Asia/Kolkata", "environments": ["dev", "prod"],
          "settings": {"schedule_timezone": "Asia/Kolkata"}, "manifest": {"kind": "platform"}, "fingerprint": "d1"}


def dataset(name="orders", **over):
    doc = {"domain": "shop", "source_name": "erp", "source_env": "prod", "dataset_name": name, "is_enabled": True,
           "kind": "dataset", "archetypes": ["PA2"], "ingest_mode": "push", "ingest_read": "change_feed",
           "history_kind": "transaction", "load_pattern": "history", "contract_id": f"c-{name}",
           "contract_version": "0.1.0", "fingerprint": f"fp-{name}",
           "settings": {"load_type": "INCREMENTAL", "retry_count": "5"}, "columns": COLUMNS,
           "business_keys": {"primary": ["order_id", "line_no"], "dedup": ["order_id"]},
           "manifest": {"dataset_name": name, "load": {"pattern": "history", "tolerance": 0.0}}}
    return {**doc, **over}


def seed(s):
    s.register_settings(DEFS)
    s.load_domain(DOMAIN)
    s.load_dataset(dataset())
    s.load_dataset(dataset("material", is_enabled=False, settings={}, business_keys={"primary": ["order_id"]}))
    return s


# ---------------------------------------------------------------------------------------------------- helpers


def started(s, run="r1", ds="orders", run_type="normal"):
    s.start_run(run, "shop", "dev", "schedule", run_type)
    return s.start_dataset_run(run, "erp", ds, "cfg1", {"retry_count": "5"})


def one_run(s, run, status, watermark=None, bookmarks=None):
    started(s, run)
    s.finish_dataset_run(run, "erp", "orders", status, {"rows": 3}, watermark, bookmarks)
    return s.finish_run(run)


def setting(s, key):
    return next((x.value, x.origin) for x in s.settings("erp", "orders") if x.key == key)


def file(run="r1", key="2026-11-04:abc", status="loaded", name="a.csv"):
    return FileRecord(run, "erp", "orders", key, name, "2026-11-04", "abc", 3, 3, status)


PLAN = BackfillPlan("bp-1", "erp", "orders", ("2026-11-12", "2026-11-13"), "2026-11-19",
                    "the net value was keyed in twice", "sales-data@shop.example")
FIX = Correction({"order_id": "SO1", "line_no": 10}, "net_value", "12.5", was="10.0")


def _refused(s, rows):
    """Record a plan whose second row is refused; the refusal must leave nothing behind."""
    try:
        s.record_corrections(PLAN, rows)
    except InvalidArgument:
        return None
    raise AssertionError("expected a refusal")


def statuses(s, plan):
    """Run r1 with one dataset run per (dataset, status) and return the run's status."""
    s.start_run("r1", "shop", "dev", "schedule", "normal")
    for ds, st in plan:
        s.start_dataset_run("r1", "erp", ds, "cfg1", {})
        s.finish_dataset_run("r1", "erp", ds, st)
    return s.finish_run("r1")


RULES = [DqRule("dq-02", "net_value", "range", {"min": 0}, "minor", "warn"),
         DqRule("dq-01", "order_id", "not_null", {}, "major", "quarantine")]


def dq_logged(s, failed=1, rule="dq-01", detail="ok"):
    s.set_dq_rules("erp", "orders", RULES)
    started(s)
    return s.log_dq_results([DqResult("r1", "erp", "orders", rule, 10, failed, "quarantine", detail)])


def seq(*calls):
    """Run calls in order; return the last result."""
    def run(s):
        out = None
        for c in calls:
            out = c(s)
        return out
    return run


SECRET = "connect failed: Password=hunter2;User=x token=abc123 Authorization: Bearer eyJ.abc-123"

# --------------------------------------------------------------------------------------------- the contract table

CASES = [
    # configuration: registry, merged view, columns, keys
    ("registry: every def kept", lambda s: len(s.settings_registry()), 5),
    ("registry: scope must be known", lambda s: s.register_settings([SettingDef("x", "galaxy", "string")]),
     InvalidArgument),
    ("datasets: listed in name order with state", lambda s: [(d.dataset, d.is_enabled) for d in s.datasets("shop")],
     [("material", False), ("orders", True)]),
    ("datasets: another domain has none", lambda s: s.datasets("bank"), []),
    ("merge: the dataset value wins", lambda s: setting(s, "retry_count"), ("5", "dataset")),
    ("merge: the domain value beats the default", lambda s: setting(s, "schedule_timezone"),
     ("Asia/Kolkata", "domain")),
    ("merge: the registry default fills the rest", lambda s: setting(s, "cost_center"), (None, "default")),
    ("merge: every registered dataset key, platform keys left out",
     lambda s: [x.key for x in s.settings("erp", "orders")],
     ["cost_center", "load_type", "retry_count", "schedule_timezone"]),
    ("merge: unknown dataset is refused", lambda s: s.settings("erp", "nope"), NotFound),
    ("columns: contract order and fields", lambda s: [(c.name, c.required, c.primary_key_position)
                                                      for c in s.columns("erp", "orders")],
     [("order_id", True, 1), ("line_no", True, 2), ("net_value", False, None)]),
    ("keys: primary in position order", lambda s: s.business_keys("erp", "orders"), ["order_id", "line_no"]),
    ("keys: dedup keys", lambda s: s.business_keys("erp", "orders", "dedup"), ["order_id"]),
    ("keys: a kind with none is empty", lambda s: s.business_keys("erp", "material", "dedup"), []),
    ("keys: unknown kind refused", lambda s: s.business_keys("erp", "orders", "natural"), InvalidArgument),
    ("view: merged settings with origins",
     lambda s: (s.dataset_view("erp", "orders")["settings"], s.dataset_view("erp", "orders")["origins"]),
     ({"load_type": "INCREMENTAL", "retry_count": "5", "schedule_timezone": "Asia/Kolkata"},
      {"load_type": "dataset", "retry_count": "dataset", "schedule_timezone": "domain"})),
    ("view: identity, keys, columns, manifest and fingerprint",
     lambda s: {k: v for k, v in s.dataset_view("erp", "orders").items() if k not in ("settings", "origins")},
     {"domain": "shop", "source": "erp", "dataset": "orders", "source_env": "prod", "is_enabled": True,
      "kind": "dataset", "archetypes": ["PA2"], "ingest_mode": "push", "ingest_read": "change_feed",
      "history_kind": "transaction", "load_pattern": "history", "contract_id": "c-orders",
      "contract_version": "0.1.0", "fingerprint": "fp-orders", "columns": COLUMNS,
      "business_keys": {"primary": ["order_id", "line_no"], "dedup": ["order_id"]},
      "manifest": {"dataset_name": "orders", "load": {"pattern": "history", "tolerance": 0.0}}}),
    ("view: unknown dataset refused", lambda s: s.dataset_view("erp", "nope"), NotFound),
    # configuration: the loader's writes and their refusals
    ("load: unregistered setting refused", lambda s: s.load_dataset(dataset(settings={"retyr_count": "5"})),
     UnknownSetting),
    ("load: a platform key is not a dataset setting",
     lambda s: s.load_dataset(dataset(settings={"default_realm": "local"})), UnknownSetting),
    ("load: business key must be a column",
     lambda s: s.load_dataset(dataset(business_keys={"primary": ["order_no"]})), InvalidArgument),
    ("load: business key kind must be known",
     lambda s: s.load_dataset(dataset(business_keys={"natural": ["order_id"]})), InvalidArgument),
    ("load: domain must be loaded first", lambda s: s.load_dataset(dataset(domain="bank")), NotFound),
    ("load: domain setting must be registered",
     lambda s: s.load_domain({**DOMAIN, "settings": {"timezone": "UTC"}}), UnknownSetting),
    ("load: a reload replaces settings whole",
     seq(lambda s: s.load_dataset(dataset(settings={"load_type": "FULL"})), lambda s: setting(s, "retry_count")),
     ("2", "default")),
    ("load: a reload keeps one row per dataset",
     seq(lambda s: s.load_dataset(dataset(fingerprint="fp-2")), lambda s: [d.fingerprint for d in s.datasets("shop")]),
     ["fp-material", "fp-2"]),
    # run log: open and close
    ("run: first dataset run has no watermark before", started, None),
    ("run: same run id twice refused", seq(started, lambda s: s.start_run("r1", "shop", "dev")), RunStateError),
    ("run: run type must be known", lambda s: s.start_run("r1", "shop", "dev", "schedule", "replay"),
     InvalidArgument),
    ("run: domain must be loaded", lambda s: s.start_run("r1", "bank", "dev"), NotFound),
    ("run: dataset run needs its run", lambda s: s.start_dataset_run("r9", "erp", "orders", "cfg1", {}), NotFound),
    ("run: dataset run needs a loaded dataset",
     seq(lambda s: s.start_run("r1", "shop", "dev"), lambda s: s.start_dataset_run("r1", "erp", "x", "c", {})),
     NotFound),
    ("run: a dataset starts once per run",
     seq(started, lambda s: s.start_dataset_run("r1", "erp", "orders", "cfg1", {})), RunStateError),
    ("run: finish needs a started dataset",
     seq(lambda s: s.start_run("r1", "shop", "dev"), lambda s: s.finish_dataset_run("r1", "erp", "orders", "failed")),
     RunStateError),
    ("run: a dataset run finishes once",
     seq(started, lambda s: s.finish_dataset_run("r1", "erp", "orders", "failed"),
         lambda s: s.finish_dataset_run("r1", "erp", "orders", "failed")), RunStateError),
    ("run: dataset end status must be known",
     seq(started, lambda s: s.finish_dataset_run("r1", "erp", "orders", "done")), InvalidArgument),
    ("run: cannot close with a dataset still running", seq(started, lambda s: s.finish_run("r1")), RunStateError),
    ("run: a run closes once", seq(lambda s: one_run(s, "r1", "succeeded"), lambda s: s.finish_run("r1")),
     RunStateError),
    ("run: no new dataset after close",
     seq(lambda s: one_run(s, "r1", "succeeded"), lambda s: s.start_dataset_run("r1", "erp", "material", "c", {})),
     RunStateError),
    ("run status: all succeeded", lambda s: statuses(s, [("orders", "succeeded"), ("material", "skipped")]),
     "succeeded"),
    ("run status: some failed is partial", lambda s: statuses(s, [("orders", "succeeded"), ("material", "failed")]),
     "partial"),
    ("run status: all failed", lambda s: statuses(s, [("orders", "failed"), ("material", "failed")]), "failed"),
    ("run status: nothing attempted is failed", lambda s: statuses(s, []), "failed"),
    # watermarks and bookmarks: move only with a succeeded run record
    ("watermark: advances on success", seq(lambda s: one_run(s, "r1", "succeeded", "100"),
                                           lambda s: s.watermark("erp", "orders")), "100"),
    ("watermark: kept on failure", seq(lambda s: one_run(s, "r1", "succeeded", "100"),
                                       lambda s: one_run(s, "r2", "failed", "200"),
                                       lambda s: s.watermark("erp", "orders")), "100"),
    ("watermark: kept when a success passes none", seq(lambda s: one_run(s, "r1", "succeeded", "100"),
                                                       lambda s: one_run(s, "r2", "succeeded", None),
                                                       lambda s: s.watermark("erp", "orders")), "100"),
    ("watermark: the next run sees it as before", seq(lambda s: one_run(s, "r1", "succeeded", "100"),
                                                      lambda s: started(s, "r2")), "100"),
    ("watermark: unknown dataset refused", lambda s: s.watermark("erp", "nope"), NotFound),
    ("bookmarks: merged on success, ignored on failure",
     seq(lambda s: one_run(s, "r1", "succeeded", None, {"shard-0": "10", "shard-1": "4"}),
         lambda s: one_run(s, "r2", "succeeded", None, {"shard-1": "9"}),
         lambda s: one_run(s, "r3", "failed", None, {"shard-0": "99"}),
         lambda s: s.bookmarks("erp", "orders")), {"shard-0": "10", "shard-1": "9"}),
    ("bookmarks: none yet", lambda s: s.bookmarks("erp", "orders"), {}),
    ("reset: audited reset moves the watermark",
     seq(lambda s: one_run(s, "r1", "succeeded", "100"),
         lambda s: s.reset_watermark("erp", "orders", "40", "backfill October after the late dealer file", "ops"),
         lambda s: s.watermark("erp", "orders")), "40"),
    ("reset: needs a real reason", lambda s: s.reset_watermark("erp", "orders", "0", "fix", "ops"), InvalidArgument),
    ("reset: needs who made it", lambda s: s.reset_watermark("erp", "orders", "0", "backfill all of October", " "),
     InvalidArgument),
    # files, steps, errors
    ("batch: never seen", lambda s: s.batch_status("erp", "orders", "2026-11-04:abc"), None),
    ("batch: the latest status wins",
     seq(started, lambda s: s.log_file(file()), lambda s: s.log_file(file(status="skipped_duplicate", name="b.csv")),
         lambda s: s.batch_status("erp", "orders", "2026-11-04:abc")), "skipped_duplicate"),
    ("batch: keys are per dataset",
     seq(started, lambda s: s.log_file(file()), lambda s: s.batch_status("erp", "material", "2026-11-04:abc")), None),
    ("file: needs a started dataset run",
     seq(lambda s: s.start_run("r1", "shop", "dev"), lambda s: s.log_file(file())), RunStateError),
    ("step: status must be known",
     seq(started, lambda s: s.log_step(StepRecord("r1", "erp", "orders", "land", "ok"))), InvalidArgument),
    ("step: needs a started dataset run",
     seq(started, lambda s: s.log_step(StepRecord("r1", "erp", "material", "land", "succeeded"))), RunStateError),
    ("error: needs its run", lambda s: s.log_error("r9", None, None, "land", "IOError", "x"), NotFound),
    ("error: secrets redacted",
     seq(started, lambda s: s.log_error("r1", "erp", "orders", "land", "ConnectError", SECRET),
         lambda s: s.run_report("r1")["errors"][0]["message"]),
     "connect failed: Password=***;User=x token=*** Authorization: Bearer ***"),
    ("error: capped", seq(started, lambda s: s.log_error("r1", None, None, "plan", "ValueError", "x" * 9000),
                          lambda s: len(s.run_report("r1")["errors"][0]["message"])), ERROR_CAP),
    ("report: unknown run refused", lambda s: s.run_report("r9"), NotFound),
    # W1b: the files a dataset has handled, the lander's outcomes only
    ("files: a lander outcome only", seq(started, lambda s: s.log_file(file(status="parsed"))), InvalidArgument),
    ("files: listed oldest first, all windows",
     seq(started, lambda s: s.log_file(file()), lambda s: s.log_file(file(key="k2", name="b.csv", status="held_mass_delete")),
         lambda s: [(f.file_name, f.status) for f in s.dataset_files("erp", "orders")]),
     [("a.csv", "loaded"), ("b.csv", "held_mass_delete")]),
    ("files: one window", seq(started, lambda s: s.log_file(file()),
                              lambda s: s.dataset_files("erp", "orders", "2026-11-05")), []),
    ("files: a record comes back whole", seq(started, lambda s: s.log_file(file()),
                                             lambda s: s.dataset_files("erp", "orders", "2026-11-04")), [file()]),
    ("files: unknown dataset refused", lambda s: s.dataset_files("erp", "nope"), NotFound),
    # W1b: corrections live in their own table, activated by a backfill plan (ADR-017, DPC-0082)
    ("corrections: recorded, not yet active",
     seq(lambda s: s.record_corrections(PLAN, [FIX]),
         lambda s: [(c.key, c.field, c.value, c.was, c.plan_id, c.status)
                    for c in s.corrections("erp", "orders", "2026-11-18")]),
     [({"order_id": "SO1", "line_no": 10}, "net_value", "12.5", "10.0", "bp-1", "recorded")]),
    ("corrections: active from the plan's date",
     seq(lambda s: s.record_corrections(PLAN, [FIX, FIX]),
         lambda s: [c.status for c in s.corrections("erp", "orders", "2026-11-19")]), ["active", "active"]),
    ("corrections: no date lists them as recorded",
     seq(lambda s: s.record_corrections(PLAN, [FIX]), lambda s: [c.status for c in s.corrections("erp", "orders")]),
     ["recorded"]),
    ("corrections: the plan is kept with its windows",
     seq(lambda s: s.record_corrections(PLAN, [FIX]), lambda s: s.backfill_plans("erp", "orders")), [PLAN]),
    ("corrections: none for another dataset",
     seq(lambda s: s.record_corrections(PLAN, [FIX]), lambda s: s.corrections("erp", "material", "2026-12-01")), []),
    ("corrections: a plan id is recorded once",
     seq(lambda s: s.record_corrections(PLAN, [FIX]), lambda s: s.record_corrections(PLAN, [FIX])), RunStateError),
    ("corrections: the key must be the primary key",
     lambda s: s.record_corrections(PLAN, [Correction({"order_id": "SO1"}, "net_value", "1")]), InvalidArgument),
    ("corrections: the field must be a column",
     lambda s: s.record_corrections(PLAN, [Correction(FIX.key, "colour", "red")]), InvalidArgument),
    ("corrections: a plan needs a real reason",
     lambda s: s.record_corrections(replace(PLAN, reason="fix"), [FIX]), InvalidArgument),
    ("corrections: a plan needs who recorded it",
     lambda s: s.record_corrections(replace(PLAN, recorded_by=" "), [FIX]), InvalidArgument),
    ("corrections: a plan names its windows",
     lambda s: s.record_corrections(replace(PLAN, windows=()), [FIX]), InvalidArgument),
    ("corrections: unknown dataset refused", lambda s: s.record_corrections(replace(PLAN, dataset="nope"), [FIX]),
     NotFound),
    ("corrections: nothing kept from a refused call",
     seq(lambda s: _refused(s, [FIX, Correction(FIX.key, "colour", "red")]), lambda s: s.backfill_plans("erp", "orders")),
     []),
    # B9: profiles (DPC-0051)
    ("profile: the newest is returned",
     seq(started, lambda s: s.save_profile("r1", "erp", "orders", {"rows": 1}),
         lambda s: s.save_profile("r1", "erp", "orders", {"rows": 2}), lambda s: s.last_profile("erp", "orders")),
     {"rows": 2}),
    ("profile: none yet", lambda s: s.last_profile("erp", "orders"), None),
    ("profile: needs a started dataset run", lambda s: s.save_profile("r9", "erp", "orders", {"rows": 1}), NotFound),
    # ORCH-04: writer leases
    ("lease: a free table is taken", lambda s: s.acquire_lease("silver:erp.orders", "r1", 60), None),
    ("lease: the same run takes it again", seq(lambda s: s.acquire_lease("silver:erp.orders", "r1", 60),
                                               lambda s: s.acquire_lease("silver:erp.orders", "r1", 60)), None),
    ("lease: a second run finds it busy", seq(lambda s: s.acquire_lease("silver:erp.orders", "r1", 60),
                                              lambda s: s.acquire_lease("silver:erp.orders", "r2", 60)), "r1"),
    ("lease: another table is free", seq(lambda s: s.acquire_lease("silver:erp.orders", "r1", 60),
                                         lambda s: s.acquire_lease("gold:sales.fact_otif", "r2", 60)), None),
    ("lease: free again after release", seq(lambda s: s.acquire_lease("silver:erp.orders", "r1", 60),
                                            lambda s: s.release_lease("silver:erp.orders", "r1"),
                                            lambda s: s.acquire_lease("silver:erp.orders", "r2", 60)), None),
    ("lease: only the holder releases", seq(lambda s: s.acquire_lease("silver:erp.orders", "r1", 60),
                                            lambda s: s.release_lease("silver:erp.orders", "r2"),
                                            lambda s: s.acquire_lease("silver:erp.orders", "r2", 60)), "r1"),
    ("lease: needs a positive time to live", lambda s: s.acquire_lease("silver:erp.orders", "r1", 0),
     InvalidArgument),
    # W1f: a loaded batch voided (ADR-025)
    ("void: a voided batch reads as void",
     seq(started, lambda s: s.log_file(file()),
         lambda s: s.void_batch("erp", "orders", "2026-11-04:abc", "short material copy slipped past", "ops@shop"),
         lambda s: s.batch_status("erp", "orders", "2026-11-04:abc")), "void"),
    ("void: the file list shows it as void",
     seq(started, lambda s: s.log_file(file()),
         lambda s: s.void_batch("erp", "orders", "2026-11-04:abc", "short material copy slipped past", "ops@shop"),
         lambda s: [f.status for f in s.dataset_files("erp", "orders")]), ["void"]),
    ("void: only a loaded batch", seq(started, lambda s: s.log_file(file(status="rejected_volume")),
                                      lambda s: s.void_batch("erp", "orders", "2026-11-04:abc", "a real reason here",
                                                             "ops@shop")), InvalidArgument),
    ("void: never twice",
     seq(started, lambda s: s.log_file(file()),
         lambda s: s.void_batch("erp", "orders", "2026-11-04:abc", "a real reason here", "ops@shop"),
         lambda s: s.void_batch("erp", "orders", "2026-11-04:abc", "a real reason here", "ops@shop")), RunStateError),
    ("void: needs an approver", seq(started, lambda s: s.log_file(file()),
                                    lambda s: s.void_batch("erp", "orders", "2026-11-04:abc", "a real reason here", " ")),
     InvalidArgument),
    ("void: needs a real reason", seq(started, lambda s: s.log_file(file()),
                                      lambda s: s.void_batch("erp", "orders", "2026-11-04:abc", "oops", "ops@shop")),
     InvalidArgument),
    # B5: data quality rules and results (DPC-0010, DPC-0042, ADR-009)
    ("dq: rules kept, by rule id",
     seq(lambda s: s.set_dq_rules("erp", "orders", RULES), lambda s: [r.rule_id for r in s.dq_rules("erp", "orders")]),
     ["dq-01", "dq-02"]),
    ("dq: params and severity kept",
     seq(lambda s: s.set_dq_rules("erp", "orders", RULES), lambda s: s.dq_rules("erp", "orders")[1]), RULES[0]),
    ("dq: setting again replaces the rules",
     seq(lambda s: s.set_dq_rules("erp", "orders", RULES), lambda s: s.set_dq_rules("erp", "orders", RULES[:1]),
         lambda s: [r.rule_id for r in s.dq_rules("erp", "orders")]), ["dq-02"]),
    ("dq: no rules is an empty list", lambda s: s.dq_rules("erp", "material"), []),
    ("dq: a rule id twice is refused",
     lambda s: s.set_dq_rules("erp", "orders", [RULES[0], RULES[0]]), InvalidArgument),
    ("dq: a column the dataset lacks is refused",
     lambda s: s.set_dq_rules("erp", "orders", [replace(RULES[0], column="colour")]), InvalidArgument),
    ("dq: an unknown action is refused",
     lambda s: s.set_dq_rules("erp", "orders", [replace(RULES[0], action="ignore")]), InvalidArgument),
    ("dq: a row-level rule needs no column",
     seq(lambda s: s.set_dq_rules("erp", "orders", [replace(RULES[0], column="", check="row_count")]),
         lambda s: s.dq_rules("erp", "orders")[0].column), ""),
    ("dq: unknown dataset refused", lambda s: s.set_dq_rules("erp", "nope", RULES), NotFound),
    ("dq: a result is logged and read back",
     seq(dq_logged, lambda s: s.dq_results("r1")),
     [DqResult("r1", "erp", "orders", "dq-01", 10, 1, "quarantine", "ok")]),
    ("dq: results filtered by dataset", seq(dq_logged, lambda s: s.dq_results("r1", "erp", "material")), []),
    ("dq: a result for an unknown rule is refused", lambda s: dq_logged(s, rule="dq-99"), InvalidArgument),
    ("dq: more failed than checked is refused", lambda s: dq_logged(s, failed=11), InvalidArgument),
    ("dq: a result needs a started dataset run",
     seq(lambda s: s.set_dq_rules("erp", "orders", RULES),
         lambda s: s.log_dq_results([DqResult("r9", "erp", "orders", "dq-01", 1, 0, "warn")])), NotFound),
    ("dq: secrets in a result detail are redacted",
     seq(lambda s: dq_logged(s, detail=SECRET), lambda s: "hunter2" in s.dq_results("r1")[0].detail), False),
    ("report: everything for one run",
     seq(started,
         lambda s: s.log_step(StepRecord("r1", "erp", "orders", "land", "succeeded", {"loaded": 1}, "ok")),
         lambda s: s.log_file(file()),
         lambda s: s.log_error("r1", "erp", "orders", "land", "SchemaError", "missing defect_code"),
         lambda s: s.finish_dataset_run("r1", "erp", "orders", "succeeded", {"rows": 3}, "7", {"feed": "7"}, "fine"),
         lambda s: s.finish_run("r1"),
         lambda s: s.run_report("r1")),
     {"run": {"run_id": "r1", "domain": "shop", "env": "dev", "trigger": "schedule", "run_type": "normal",
              "status": "succeeded"},
      "datasets": [{"source": "erp", "dataset": "orders", "status": "succeeded", "config_fingerprint": "cfg1",
                    "watermark_before": None, "watermark_after": "7", "counts": {"rows": 3}, "message": "fine"}],
      "steps": [{"source": "erp", "dataset": "orders", "step": "land", "status": "succeeded",
                 "counts": {"loaded": 1}, "message": "ok"}],
      "files": [{"source": "erp", "dataset": "orders", "batch_key": "2026-11-04:abc", "file_name": "a.csv",
                 "window": "2026-11-04", "checksum": "abc", "declared_rows": 3, "rows": 3, "status": "loaded",
                 "reason": "", "load_type": "incremental"}],
      "errors": [{"source": "erp", "dataset": "orders", "step": "land", "error_class": "SchemaError",
                  "message": "missing defect_code"}]}),
]

# ------------------------------------------------------------------------------------------------------ adapters

PG_DSN = os.environ.get("DP_META_TEST_DSN", "dbname=dp_meta_test")
_pg = {}


def _pg_conn():
    if "conn" not in _pg:
        try:
            import psycopg

            from dataplatform.adapters.postgres_meta import create_database

            create_database(PG_DSN)
            _pg["conn"] = psycopg.connect(PG_DSN, autocommit=True)
        except Exception as exc:  # noqa: BLE001 - any failure to reach a server means skip, with the reason
            _pg["conn"] = None
            _pg["why"] = str(exc)
    if _pg["conn"] is None:
        pytest.skip(f"no local PostgreSQL: {_pg['why']}")
    return _pg["conn"]


def fresh_postgres():
    from dataplatform.adapters.postgres_meta import PostgresMetaStore, install

    conn = _pg_conn()
    conn.execute("DROP SCHEMA IF EXISTS config, runtime CASCADE")
    conn.autocommit = False
    install(conn)
    conn.autocommit = True
    return PostgresMetaStore(conn)


STORES = {"memory": MemoryMetaStore, "postgres": fresh_postgres}


@pytest.mark.parametrize("adapter", list(STORES))
@pytest.mark.parametrize(("case", "act", "expect"), CASES, ids=[c[0] for c in CASES])
def test_metadata_store_contract(adapter, case, act, expect):
    store = seed(STORES[adapter]())
    if isinstance(expect, type) and issubclass(expect, Exception):
        with pytest.raises(expect):
            act(store)
    else:
        assert act(store) == expect


# ------------------------------------------------------------------ PostgreSQL only: the procedures are the way in

ADAPTER = Path(__file__).parents[2] / "src/dataplatform/adapters/postgres_meta.py"
KERNEL = Path(__file__).parents[2] / "src/dataplatform/meta"


def test_adapter_only_calls_procedures():
    """DPC-0033, static: every statement the service adapter sends is a call to a config./runtime. function."""
    service = ADAPTER.read_text().split("class PostgresMetaStore")[1]  # install() and create_database() are setup
    calls = re.findall(r'"(SELECT[^"]*)"', service)
    assert calls, "found no statements"
    for sql in calls:
        assert re.fullmatch(r"SELECT (\* FROM )?(config|runtime)\.[a-z_]+\((%s(::jsonb)?(, )?)*\)", sql), sql


@pytest.mark.parametrize("path", ["port.py", "registry.py", "loader.py", "__init__.py"])
def test_no_product_names_in_the_kernel(path):
    """R1: the port, registry and loader name no product."""
    products = r"\b(postgres|postgresql|psycopg|sqlite|lakebase|aurora|databricks|aws|azure)\b"
    assert not re.findall(products, (KERNEL / path).read_text(), flags=re.IGNORECASE)


ROLE = "dp_meta_test_service"


@pytest.mark.parametrize(
    ("case", "sql", "allowed"),
    [
        ("call a reader procedure", "SELECT count(*) FROM config.list_datasets('shop')", True),
        ("call a writer procedure", "SELECT runtime.start_run('r1', 'shop', 'dev', 'x', 'normal')", True),
        ("read a config table", "SELECT count(*) FROM config.dataset", False),
        ("read a runtime table", "SELECT count(*) FROM runtime.watermark", False),
        ("write a watermark directly", "INSERT INTO runtime.watermark (dataset_id, watermark_value) VALUES (1, 'x')",
         False),
    ],
)
def test_service_role_reaches_only_the_procedures(case, sql, allowed):
    """DPC-0033, dynamic: a service role granted by install() can call procedures and touch no table."""
    store = seed(fresh_postgres())  # skips when no server answers
    import psycopg

    from dataplatform.adapters.postgres_meta import install

    conn = store.conn
    if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (ROLE,)).fetchone():
        conn.execute(f'CREATE ROLE "{ROLE}" NOLOGIN')
    conn.autocommit = False
    install(conn, service_role=ROLE)
    try:
        conn.execute(f'SET ROLE "{ROLE}"')
        if allowed:
            conn.execute(sql)
        else:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(sql)
    finally:
        conn.rollback()
        conn.autocommit = True
