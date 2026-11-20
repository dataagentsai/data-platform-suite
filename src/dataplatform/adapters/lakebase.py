"""The metadata store on Lakebase, Databricks' managed PostgreSQL (ADR-010, binding dbs/engine/databricks.yaml).

A job on Databricks connects as its own identity: the Databricks SDK client (on job compute it authenticates as the
job's service principal with no configuration) looks up the instance's address and mints a short-lived database
token, which is passed to the connection as its password and nothing else: not an environment variable, not a file,
not a log line (DPC-0029). A connection stays open after its token expires (the token is checked at login only), so
a run opens one connection at its start; a run longer than the token's life opens a new one through connect() again.

    store = connect_lakebase("dp-meta-dev", "dp_meta")             # on Databricks job compute
    store = meta_store_from_env()                                  # DP_META_INSTANCE, else DP_META_DSN, else local
"""

from __future__ import annotations

import os
import uuid
from typing import Any

from dataplatform.adapters.postgres_meta import PostgresMetaStore


def _project_endpoint(client: Any, name: str) -> tuple[str, str] | None:
    """(endpoint resource name, host) of a Lakebase project's read-write endpoint on its default branch, or None when
    the workspace has no such project. Lakebase on Azure Databricks is the project (autoscaling) kind, found over REST
    so an older SDK works too; the first cloud's provisioned instances are the fallback (first real Azure run, 19 Nov
    2026: the instance endpoint answered "Resource not found" for a project)."""
    api = getattr(client, "api_client", None)
    if api is None:
        return None
    try:
        reply = api.do("GET", f"/api/2.0/postgres/projects/{name}/branches/production/endpoints")
    except Exception as exc:  # noqa: BLE001 -- the SDK's NotFound, found by name so the SDK is never imported here
        if type(exc).__name__ == "NotFound":
            return None
        raise
    for ep in reply.get("endpoints", []):
        status = ep.get("status", {})
        host = status.get("hosts", {}).get("host")
        if status.get("endpoint_type") == "ENDPOINT_TYPE_READ_WRITE" and host:
            return str(ep["name"]), str(host)
    return None


def instance_dsn(client: Any, instance: str, database: str) -> str:
    """host, port, database and user for the instance; no password (the token is passed separately)."""
    project = _project_endpoint(client, instance)
    if project is not None:
        user = client.current_user.me().user_name
        return f"host={project[1]} port=5432 dbname={database} user={user} sslmode=require"
    if hasattr(client, "database"):
        host = client.database.get_database_instance(name=instance).read_write_dns
    else:  # an older SDK (serverless environment 3, probed 13 Nov) has no database API: the REST endpoint instead
        host = client.api_client.do("GET", f"/api/2.0/database/instances/{instance}")["read_write_dns"]
    user = client.current_user.me().user_name
    return f"host={host} port=5432 dbname={database} user={user} sslmode=require"


def instance_token(client: Any, instance: str) -> str:
    project = _project_endpoint(client, instance)
    if project is not None:
        return str(client.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": project[0]})["token"])
    request_id = str(uuid.uuid4())
    if hasattr(client, "database"):
        cred = client.database.generate_database_credential(instance_names=[instance], request_id=request_id)
        return str(cred.token)
    body = {"instance_names": [instance], "request_id": request_id}
    return str(client.api_client.do("POST", "/api/2.0/database/credentials", body=body)["token"])


def connect_lakebase(instance: str, database: str = "dp_meta", client: Any = None,
                     connect: Any = None) -> PostgresMetaStore:
    """A metadata store on a Lakebase instance, connected as the caller's own identity."""
    if client is None:
        from databricks.sdk import WorkspaceClient  # noqa: PLC0415 -- only on Databricks or with the SDK installed

        client = WorkspaceClient()
    if connect is None:
        import psycopg  # noqa: PLC0415 -- the PostgreSQL driver, as in postgres_meta

        connect = psycopg.connect
    conn = connect(instance_dsn(client, instance, database), password=instance_token(client, instance))
    conn.autocommit = True  # each procedure call is its own transaction, as PostgresMetaStore.connect sets
    return PostgresMetaStore(conn)


def meta_store_from_env(client: Any = None, connect: Any = None) -> Any:
    """The binding's choice, from the environment: DP_META_INSTANCE (+ DP_META_DATABASE) means Lakebase as this
    identity; otherwise DP_META_DSN (plain PostgreSQL, the laptop and other clouds); one adapter either way."""
    instance = os.environ.get("DP_META_INSTANCE")
    if instance:
        return connect_lakebase(instance, os.environ.get("DP_META_DATABASE", "dp_meta"), client, connect)
    return PostgresMetaStore.connect()
