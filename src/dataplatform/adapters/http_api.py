"""RecordInterface as a small local HTTP service: the dealer management system.

    LocalRecordStore   the records behind the interface (SQLite file, standard library), written by the world
    serve(store, port) a standard-library HTTP server:  GET /<system>/<resource>?cursor=<c>&limit=<n>
                       -> {"items": [...], "next_cursor": "<c>"}
    HttpRecordClient   the same port read over HTTP, so contract tests prove the wire and the store agree

The cursor is "<updated_at>|<key>": records come oldest change first, ties broken by key, so paging never skips
or repeats a record even when many share a timestamp.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

MAX_PAGE = 200


class LocalRecordStore:
    def __init__(self, path: str | Path = ":memory:"):
        self.path = str(path)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.lock = threading.Lock()
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS records (system TEXT, resource TEXT, key TEXT, updated_at TEXT, "
            "body TEXT, PRIMARY KEY (system, resource, key))"
        )
        self.conn.commit()

    def reset(self) -> None:
        with self.lock:
            self.conn.execute("DELETE FROM records")
            self.conn.commit()

    def upsert(self, system: str, resource: str, key: str, record: dict[str, Any], updated_at: str) -> None:
        with self.lock:
            self.conn.execute(
                "INSERT INTO records VALUES (?, ?, ?, ?, ?) ON CONFLICT (system, resource, key) "
                "DO UPDATE SET updated_at = excluded.updated_at, body = excluded.body",
                (system, resource, key, updated_at, json.dumps(record, sort_keys=True)),
            )
            self.conn.commit()

    def page(self, system: str, resource: str, cursor: str, limit: int) -> tuple[list[dict[str, Any]], str]:
        limit = max(1, min(limit, MAX_PAGE))
        ts, _, key = cursor.partition("|")
        with self.lock:
            rows = self.conn.execute(
                "SELECT key, updated_at, body FROM records WHERE system = ? AND resource = ? "
                "AND (updated_at > ? OR (updated_at = ? AND key > ?)) ORDER BY updated_at, key LIMIT ?",
                (system, resource, ts, ts, key, limit),
            ).fetchall()
        if not rows:
            return [], cursor
        return [json.loads(b) for _, _, b in rows], f"{rows[-1][1]}|{rows[-1][0]}"


def _handler(store: LocalRecordStore) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - name fixed by http.server
            url = urllib.parse.urlparse(self.path)
            parts = [p for p in url.path.split("/") if p]
            if len(parts) != 2:
                self._send(404, {"error": "use /<system>/<resource>"})
                return
            q = urllib.parse.parse_qs(url.query)
            try:
                limit = int(q.get("limit", [str(MAX_PAGE)])[0])
            except ValueError:
                self._send(400, {"error": "limit must be a number"})
                return
            items, nxt = store.page(parts[0], parts[1], q.get("cursor", [""])[0], limit)
            self._send(200, {"items": items, "next_cursor": nxt})

        def _send(self, code: int, body: dict[str, Any]) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args: Any) -> None:
            pass

    return Handler


def serve(store: LocalRecordStore, port: int = 0, host: str = "127.0.0.1") -> ThreadingHTTPServer:
    """Start the interface in a background thread; port 0 picks a free port (server.server_address[1])."""
    server = ThreadingHTTPServer((host, port), _handler(store))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


_DIRECT = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class HttpRecordClient:
    """Reads the interface over HTTP. Writes go to the store: a real dealer system is not written by its readers."""

    def __init__(self, base_url: str, store: LocalRecordStore | None = None):
        self.base = base_url.rstrip("/")
        self.store = store

    def upsert(self, system: str, resource: str, key: str, record: dict[str, Any], updated_at: str) -> None:
        if self.store is None:
            raise PermissionError("this interface is read-only over HTTP")
        self.store.upsert(system, resource, key, record, updated_at)

    def page(self, system: str, resource: str, cursor: str, limit: int) -> tuple[list[dict[str, Any]], str]:
        qs = urllib.parse.urlencode({"cursor": cursor, "limit": limit})
        url = f"{self.base}/{urllib.parse.quote(system)}/{urllib.parse.quote(resource)}?{qs}"
        if not url.startswith(("http://127.0.0.1", "http://localhost")):
            raise ValueError("the local client only reads the local interface")
        # no proxy: the stand-in runs on this machine, and a proxy set for the environment (Databricks serverless sets
        # one) refused the loopback call on 13 Nov
        with _DIRECT.open(url, timeout=10) as resp:  # nosec B310 - local http only, checked above
            body = json.loads(resp.read())
        return body["items"], body["next_cursor"]
