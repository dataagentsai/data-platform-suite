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
    assert client.calls == [("GET", "/api/2.0/database/instances/dp-meta-dev"), ("POST", "/api/2.0/database/credentials")]
