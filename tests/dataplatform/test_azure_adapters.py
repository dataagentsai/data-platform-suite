"""Azure Blob adapters beyond the shared contract tables (those are in test_cloud_adapters.py, test_source_ports.py and
test_secrets.py, where the azure rows sit beside the AWS and local ones).

TESTED AGAINST A FAKE OF THE SDK SURFACE ONLY (tests/dataplatform/azure_fakes.py). Not against Azurite and not against a
real Azure account. Needs no Azure SDK, boto3 or moto.
"""
from __future__ import annotations

from typing import Any

import pytest
from azure_fakes import FakeContainer, ResourceExistsError, ResourceNotFoundError

from dataplatform.adapters.azure_blob import AzureFileDrop, AzureLanding, AzureObjectStore, account_name
from dataplatform.cloud_ports import LAYERS

# (case, prefix, env, account or the error). One storage account per environment; lowercase letters and digits, 3-24.
ACCOUNT_CASES = [
    ("prefix and env", "acmedp", "dev", "acmedpdev"),
    ("digits allowed", "dp2", "prod1", "dp2prod1"),
    ("exactly 3", "d", "ev", "dev"),
    ("exactly 24", "a" * 20, "prod", "a" * 20 + "prod"),
    ("too short", "d", "v", ValueError),
    ("too long by one", "a" * 21, "prod", ValueError),
    ("uppercase is refused, not folded", "AcmeDp", "dev", ValueError),
    ("hyphen refused", "acme-dp", "dev", ValueError),
    ("underscore refused", "acme", "_dev", ValueError),
    ("empty", "", "", ValueError),
]


@pytest.mark.parametrize(("case", "prefix", "env", "want"), ACCOUNT_CASES, ids=[c[0] for c in ACCOUNT_CASES])
def test_account_name(case: str, prefix: str, env: str, want: Any) -> None:
    """ORCH-05: the account that holds an environment's layers has one valid, predictable name."""
    if isinstance(want, type):
        with pytest.raises(want, match="3 to 24 characters, lowercase letters and digits only"):
            account_name(prefix, env)
    else:
        assert account_name(prefix, env) == want


# (case, env, layer, versioning of the account, url or the error)
STORE_CASES = [
    ("dev landing", "dev", "landing", True, "abfss://landing@acmedpdev.dfs.core.windows.net/"),
    ("prod gold", "prod", "gold", True, "abfss://gold@acmedpprod.dfs.core.windows.net/"),
    ("quarantine without versioning", "dev", "quarantine", False, "abfss://quarantine@acmedpdev.dfs.core.windows.net/"),
    ("unknown layer", "dev", "platinum", False, ValueError),
    ("env that breaks the account name", "Dev", "gold", False, ValueError),
]


@pytest.mark.parametrize(("case", "env", "layer", "versioning", "want"), STORE_CASES, ids=[c[0] for c in STORE_CASES])
def test_azure_object_store(case: str, env: str, layer: str, versioning: bool, want: Any) -> None:
    """ORCH-05: the location is one container per layer in one account per environment, and "versioned" is whatever
    the account-level setting says (asked by account name, once per question)."""
    asked: list[str] = []

    def versioning_of(account: str) -> bool:
        asked.append(account)
        return versioning

    store = AzureObjectStore("acmedp", versioning_of)
    if isinstance(want, type):
        with pytest.raises(want):
            store.location(env, layer)
        with pytest.raises(want):
            store.versioned(env, layer)
        assert asked == []
    else:
        assert store.location(env, layer) == want
        assert store.versioned(env, layer) is versioning
        assert asked == [account_name("acmedp", env)]


@pytest.mark.parametrize("layer", LAYERS)
def test_every_layer_is_a_valid_container_name(layer: str) -> None:
    """ORCH-05: containers are named exactly by layer, so every layer must be a legal container name (3-63 characters,
    lowercase letters, digits, hyphen)."""
    url = AzureObjectStore("acmedp", lambda a: True).location("dev", layer)
    assert url.startswith(f"abfss://{layer}@")
    assert 3 <= len(layer) <= 63
    assert layer == layer.lower()
    assert layer.replace("-", "").isalnum()


# (case, first upload, second upload, overwrite flag of the second, outcome)
FAKE_CASES = [
    ("overwrite replaces", b"a", b"b", True, b"b"),
    ("no overwrite refuses", b"a", b"b", False, ResourceExistsError),
]


@pytest.mark.parametrize(("case", "first", "second", "overwrite", "want"), FAKE_CASES, ids=[c[0] for c in FAKE_CASES])
def test_fake_container_is_strict(case: str, first: bytes, second: bytes, overwrite: bool, want: Any) -> None:
    """The fake itself: it refuses what the service refuses, so the adapter tests mean something."""
    c = FakeContainer()
    c.upload_blob("x", first)
    if isinstance(want, type):
        with pytest.raises(want):
            c.upload_blob("x", second, overwrite=overwrite)
    else:
        c.upload_blob("x", second, overwrite=overwrite)
        assert c.download_blob("x").readall() == want


def test_fake_container_names_and_metadata() -> None:
    c = FakeContainer()
    c.upload_blob("B", b"1")
    c.upload_blob("a", b"2")
    assert [b.name for b in c.list_blobs()] == ["B", "a"]                       # lexicographic, case-sensitive
    assert [b.name for b in c.list_blobs(name_starts_with="b")] == []
    with pytest.raises(ResourceNotFoundError):
        c.download_blob("b")
    for bad in ({"arrived-at": "x"}, {"1x": "x"}, {"ok": 1}):
        with pytest.raises(ValueError, match="metadata"):
            c.upload_blob("m", b"", overwrite=True, metadata=bad)  # type: ignore[arg-type]


# (case, a second mover writing the manifest between our check and our upload) -> still refused as already landed
def test_landing_manifest_race_is_refused() -> None:
    """DUP-02: if another writer lands the manifest after our check, the service refuses the second manifest and the
    adapter reports DeliveryExists, not the SDK's error."""
    from dataplatform.ingest.ports import DeliveryExists

    c = FakeContainer()
    store = AzureLanding(c)
    real = c.list_blobs
    c.list_blobs = lambda name_starts_with=None: (  # type: ignore[method-assign]
        c.upload_blob("s/d/w/n.jsonl.manifest.json", b"{}") or real(name_starts_with="__none__"))
    with pytest.raises(DeliveryExists, match="already landed"):
        store.put("s", "d", "w", "n.jsonl", b"{}\n", 1)


# (case, arrived_at as given, stored as) - the declared arrival time is kept, in UTC
ARRIVAL_CASES = [
    ("utc", "2026-11-09T01:00:00+00:00", "2026-11-09T01:00:00+00:00"),
    ("offset is normalised", "2026-11-09T03:00:00+02:00", "2026-11-09T01:00:00+00:00"),
]


@pytest.mark.parametrize(("case", "given", "stored"), ARRIVAL_CASES, ids=[c[0] for c in ARRIVAL_CASES])
def test_file_drop_arrival_travels_as_metadata(case: str, given: str, stored: str) -> None:
    """The arrival time and checksum are blob metadata under identifier keys (underscores)."""
    c = FakeContainer()
    f = AzureFileDrop(c, prefix="p/").put("sys", "ds", "a.csv", b"x", given)
    assert f.arrived_at == stored
    assert c.get_blob_properties("p/sys/ds/a.csv").metadata == {"arrived_at": stored, "checksum": f.checksum}


def test_file_drop_without_metadata_falls_back_to_last_modified() -> None:
    """A file put by someone else (no metadata) is listed with the service's own upload time and an empty checksum."""
    c = FakeContainer()
    c.upload_blob("sys/ds/other.csv", b"x")
    (f,) = AzureFileDrop(c).list("sys", "ds")
    assert (f.name, f.size, f.checksum) == ("other.csv", 1, "")
    assert f.arrived_at.endswith("+00:00")
