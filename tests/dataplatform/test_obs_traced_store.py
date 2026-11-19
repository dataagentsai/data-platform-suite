"""A traced store (B10, ADR-024): every call forwarded, spans per run/dataset/step, hooks pushed when a run ends."""

from __future__ import annotations

from typing import Any

from dataplatform.adapters.memory_meta import MemoryMetaStore
from dataplatform.meta import registry
from dataplatform.meta.port import StepRecord
from dataplatform.obs.trace import LocalTracer
from dataplatform.obs.traced_store import TracedStore


def _store() -> MemoryMetaStore:
    s = MemoryMetaStore()
    s.register_settings(registry.load())
    s.load_domain({"domain": "t", "settings": {}, "manifest": {}, "fingerprint": "d"})
    s.load_dataset({"domain": "t", "source_name": "erp", "source_env": "", "dataset_name": "orders",
                    "is_enabled": True, "kind": "dataset", "archetypes": [], "ingest_mode": "push",
                    "ingest_read": "files", "history_kind": "event", "load_pattern": "append", "contract_id": "c",
                    "contract_version": "1", "fingerprint": "fp", "settings": {},
                    "columns": [{"name": "id", "logical_type": "string", "required": True,
                                 "primary_key_position": 1, "description": "", "classification": ""}],
                    "business_keys": {"primary": ["id"]}, "manifest": {}})
    return s


def test_a_run_is_traced_and_pushed() -> None:
    tracer, finished = LocalTracer(), []
    s = TracedStore(_store(), tracer, on_finish=(finished.append,))
    s.start_run("r1", "t", "dev", "schedule")
    s.start_dataset_run("r1", "erp", "orders", "fp", {})
    s.log_step(StepRecord("r1", "erp", "orders", "land", "succeeded", {"rows": 3}, ""))
    s.finish_dataset_run("r1", "erp", "orders", "succeeded", {"rows": 3})
    assert s.finish_run("r1") == "succeeded"
    assert finished == ["r1"]  # pushed as the run ended, not polled
    names = [sp.name for sp in tracer.finished]
    assert names[-1] == "run.schedule"  # the run's span ends last, after its dataset and steps
    assert "dataset.erp.orders" in names
    assert "step.land" in names
    assert all(sp.attrs.get("dp_run_id") == "r1" for sp in tracer.finished)
    assert len({sp.trace_id for sp in tracer.finished}) == 1  # one trace for the whole run
    assert s.run_report("r1")["run"]["status"] == "succeeded"  # everything else goes straight through


def test_untraced_calls_still_work(tmp_path: Any) -> None:
    s = TracedStore(_store(), LocalTracer())
    assert s.dataset_view("erp", "orders")["dataset"] == "orders"
