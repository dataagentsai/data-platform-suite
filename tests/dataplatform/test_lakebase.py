"""The Lakebase adapter (ADR-010): the connection string and token come from the workspace client, the token is
passed as the password only; table-driven with a stand-in client, no network."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from dataplatform.adapters.lakebase import connect_lakebase, instance_dsn, meta_store_from_env


class Client:
    def __init__(self) -> None:
        self.minted: list[list[str]] = []
        self.database = SimpleNamespace(
            get_database_instance=lambda name: SimpleNamespace(read_write_dns=f"{name}.db.example"),
            generate_database_credential=self._mint)
        self.current_user = SimpleNamespace(me=lambda: SimpleNamespace(user_name="sp-dataplatform"))

    def _mint(self, instance_names: list[str], request_id: str) -> Any:
        self.minted.append(instance_names)
        return SimpleNamespace(token="t0ken")


class Conn:
    autocommit = False


def test_dsn_has_no_password() -> None:
    dsn = instance_dsn(Client(), "dp-meta-dev", "dp_meta")
    assert dsn == "host=dp-meta-dev.db.example port=5432 dbname=dp_meta user=sp-dataplatform sslmode=require"
    assert "password" not in dsn


def test_the_token_is_the_password_and_nowhere_else(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def connect(dsn: str, password: str) -> Conn:
        seen.update(dsn=dsn, password=password)
        return Conn()

    client = Client()
    store = connect_lakebase("dp-meta-dev", "dp_meta", client, connect)
    assert seen["password"] == "t0ken"
    assert "t0ken" not in seen["dsn"]
    assert client.minted == [["dp-meta-dev"]]
    assert store.conn.autocommit is True
    assert "PGPASSWORD" not in __import__("os").environ


CASES = [
    ("an instance means Lakebase", {"DP_META_INSTANCE": "dp-meta-dev"}, "lakebase"),
    ("an instance and a database", {"DP_META_INSTANCE": "dp-meta-dev", "DP_META_DATABASE": "other"}, "lakebase"),
]


@pytest.mark.parametrize(("case", "env", "want"), CASES, ids=[c[0] for c in CASES])
def test_store_from_env(case: str, env: dict[str, str], want: str, monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    seen: dict[str, str] = {}
    meta_store_from_env(Client(), lambda dsn, password: seen.setdefault("dsn", dsn) and Conn())
    assert f"dbname={env.get('DP_META_DATABASE', 'dp_meta')}" in seen["dsn"]


class OldSdk:
    """A workspace client with no database API (serverless environment 3): the REST endpoints instead."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.api_client = SimpleNamespace(do=self._do)
        self.current_user = SimpleNamespace(me=lambda: SimpleNamespace(user_name="sp-dataplatform"))

    def _do(self, method: str, path: str, body: Any = None) -> dict[str, str]:
        self.calls.append((method, path))
        return {"read_write_dns": "h.example"} if method == "GET" else {"token": "t1"}


def test_an_older_sdk_uses_the_rest_endpoints() -> None:
    client, seen = OldSdk(), {}
    connect_lakebase("dp-meta-dev", "dp_meta", client, lambda dsn, password: seen.update(p=password) or Conn())
    assert seen["p"] == "t1"
    project = ("GET", "/api/2.0/postgres/projects/dp-meta-dev/branches/production/endpoints")  # asked first, twice
    instance = ("GET", "/api/2.0/database/instances/dp-meta-dev")
    assert client.calls == [project, instance, project, ("POST", "/api/2.0/database/credentials")]


class NotFound(Exception):
    """Stands for databricks.sdk.errors.NotFound (same name on purpose)."""


class ProjectSdk:
    """A client of a workspace that has Lakebase projects (Azure Databricks): GET lists the project's endpoints."""

    def __init__(self, endpoints: list[dict[str, Any]] | None) -> None:
        self.calls: list[tuple[str, str, Any]] = []
        self.endpoints = endpoints
        self.api_client = SimpleNamespace(do=self._do)
        self.current_user = SimpleNamespace(me=lambda: SimpleNamespace(user_name="dev.user@example.org"))

    def _do(self, method: str, path: str, body: Any = None) -> dict[str, Any]:
        self.calls.append((method, path, body))
        if method == "GET" and "/postgres/projects/" in path:
            if self.endpoints is None:
                raise NotFound("Resource not found")
            return {"endpoints": self.endpoints}
        if method == "GET":
            return {"read_write_dns": "inst.example"}
        return {"token": "t2"}


def _endpoint(kind: str, host: str | None, name: str = "projects/dp-meta-dev/branches/production/endpoints/primary"):
    return {"name": name, "status": {"endpoint_type": kind, "hosts": {"host": host} if host else {}}}


# (case, endpoints, want host or None, want token call)
PROJECT_CASES = [
    ("read-write endpoint", [_endpoint("ENDPOINT_TYPE_READ_WRITE", "ep.example")], "ep.example", True),
    ("read-only endpoints are skipped", [_endpoint("ENDPOINT_TYPE_READ_ONLY", "ro.example"),
                                         _endpoint("ENDPOINT_TYPE_READ_WRITE", "rw.example")], "rw.example", True),
    ("endpoint with no host yet falls back", [_endpoint("ENDPOINT_TYPE_READ_WRITE", None)], "inst.example", False),
    ("no such project falls back to the instance", None, "inst.example", False),
]


@pytest.mark.parametrize(("case", "endpoints", "host", "project"), PROJECT_CASES, ids=[c[0] for c in PROJECT_CASES])
def test_project_or_instance(case: str, endpoints: Any, host: str, project: bool) -> None:
    """Lakebase on Azure is a project; the first cloud's is an instance. The adapter finds which, by asking."""
    client = ProjectSdk(endpoints)
    dsn = instance_dsn(client, "dp-meta-dev", "dp_meta")
    assert dsn == f"host={host} port=5432 dbname=dp_meta user=dev.user@example.org sslmode=require"
    from dataplatform.adapters.lakebase import instance_token

    token = instance_token(client, "dp-meta-dev")
    posts = [c for c in client.calls if c[0] == "POST"]
    if project:
        assert token == "t2"
        assert posts == [("POST", "/api/2.0/postgres/credentials",
                          {"endpoint": "projects/dp-meta-dev/branches/production/endpoints/primary"})]
    else:
        assert posts[0][1] == "/api/2.0/database/credentials"
