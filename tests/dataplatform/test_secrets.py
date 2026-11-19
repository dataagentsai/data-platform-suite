"""secret_store (DPC-0029, B11; REL-05: no secret in the repository): references only, values never in errors; the AWS
adapter on moto.

The port's name grammar is the common subset of the clouds (letters, digits, hyphen, "/"; no "--"). Key Vault is tested
against a fake of the SDK surface only (tests/dataplatform/azure_fakes.py), not against Azurite and not against a real
Azure account.
"""

from __future__ import annotations

from typing import Any

import pytest
from azure_fakes import FakeKeyVault

from dataplatform.adapters.secrets import AwsSecretsManager, AzureKeyVault, EnvSecrets
from dataplatform.cloud_ports import SecretNotFound, secret_name

REF_CASES = [
    ("a plain name", "secret://erp-db/password", "erp-db/password"),
    ("not a reference", "hunter2", ValueError),
    ("empty name", "secret://", ValueError),
    ("odd characters refused", "secret://a b;c", ValueError),
    ("underscore is not portable", "secret://erp_db", ValueError),
    ("dot is not portable", "secret://erp.db", ValueError),
    ("double hyphen is not portable", "secret://erp--db", ValueError),
    ("non-ASCII letter refused", "secret://caf\u00e9", ValueError),
    ("digits and single hyphens", "secret://erp-2/db-9", "erp-2/db-9"),
    ("hyphen before a slash is not portable (a-/b and a/-b would map to one name)", "secret://a-/b", ValueError),
    ("hyphen after a slash is not portable", "secret://a/-b", ValueError),
]


@pytest.mark.parametrize(("case", "ref", "want"), REF_CASES, ids=[c[0] for c in REF_CASES])
def test_secret_name(case: str, ref: str, want: Any) -> None:
    if isinstance(want, type):
        with pytest.raises(want):
            secret_name(ref)
    else:
        assert secret_name(ref) == want


def test_env_then_file(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DP_SECRET_ERP_DB_PASSWORD", "from-env")
    (tmp_path / "mes").write_text("from-file\n")
    s = EnvSecrets(tmp_path)
    assert s.get("secret://erp-db/password") == "from-env"
    assert s.get("secret://mes") == "from-file"
    with pytest.raises(SecretNotFound, match="secret://nope") as err:
        s.get("secret://nope")
    assert "from-" not in str(err.value)


def test_aws_secrets_manager_on_moto(monkeypatch: pytest.MonkeyPatch) -> None:
    boto3 = pytest.importorskip("boto3")
    moto = pytest.importorskip("moto")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-2")
    with moto.mock_aws():
        client = boto3.client("secretsmanager")
        client.create_secret(Name="erp-db/password", SecretString="s3cr3t")
        s = AwsSecretsManager(client)
        assert s.get("secret://erp-db/password") == "s3cr3t"
        with pytest.raises(SecretNotFound, match="secret://missing"):
            s.get("secret://missing")


# adapter -> builder of a store that holds erp-db/password = s3cr3t (the shared cases below read the same reference)
def _aws_store(monkeypatch: pytest.MonkeyPatch) -> Any:
    boto3 = pytest.importorskip("boto3")
    moto = pytest.importorskip("moto")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-2")
    mock = moto.mock_aws()
    mock.start()
    client = boto3.client("secretsmanager")
    client.create_secret(Name="erp-db/password", SecretString="s3cr3t")
    return AwsSecretsManager(client), mock.stop


def _azure_store(monkeypatch: pytest.MonkeyPatch) -> Any:
    return AzureKeyVault(FakeKeyVault({"erp-db--password": "s3cr3t"})), lambda: None


STORES = {"aws": _aws_store, "azure": _azure_store}
GET_CASES = [
    ("a stored secret", "secret://erp-db/password", "s3cr3t"),
    ("a missing secret", "secret://missing", SecretNotFound),
    ("a missing nested secret", "secret://erp-db/other", SecretNotFound),
    ("a value is not a reference", "s3cr3t", ValueError),
]


@pytest.mark.parametrize("adapter", STORES)
@pytest.mark.parametrize(("case", "ref", "want"), GET_CASES, ids=[c[0] for c in GET_CASES])
def test_secret_store_contract(monkeypatch: pytest.MonkeyPatch, adapter: str, case: str, ref: str, want: Any) -> None:
    """REL-05: every secret_store adapter resolves a reference at run time and, when it cannot, names the reference and
    never a value."""
    store, stop = STORES[adapter](monkeypatch)
    try:
        if isinstance(want, type):
            with pytest.raises(want) as err:
                store.get(ref)
            assert "s3cr3t" not in str(err.value)
            if want is SecretNotFound:
                assert ref in str(err.value)
        else:
            assert store.get(ref) == want
    finally:
        stop()


# ref -> (name sent to Key Vault, or the error). The port grammar already refuses "--" and odd characters (REF_CASES);
# these are the cases only the Key Vault mapping adds. Key Vault names: letters, digits, hyphen, 1 to 127.
KV_NAME_CASES = [
    ("slash becomes two hyphens", "secret://a/b", "a--b"),
    ("deeper path", "secret://erp-db/prod/password", "erp-db--prod--password"),
    ("no slash, unchanged", "secret://plain-name", "plain-name"),
    ("a name that already has -- is refused", "secret://a--b", ValueError),
    ("a bad character is refused", "secret://a_b", ValueError),
    ("hyphen before a slash is ambiguous", "secret://a-/b", ValueError),
    ("hyphen after a slash is ambiguous", "secret://a/-b", ValueError),
    ("longest that fits", "secret://" + "a" * 127, "a" * 127),
    ("one too long", "secret://" + "a" * 128, ValueError),
    ("too long once slashes widen", "secret://" + "/".join(["a"] * 44), ValueError),
]


@pytest.mark.parametrize(("case", "ref", "want"), KV_NAME_CASES, ids=[c[0] for c in KV_NAME_CASES])
def test_key_vault_name_mapping(case: str, ref: str, want: Any) -> None:
    """REL-05: the name sent to Key Vault is the reference's name with / written as --; names it cannot hold, or that
    would collide with another, are refused before any call."""
    if isinstance(want, type):
        vault = FakeKeyVault()
        with pytest.raises(want):
            AzureKeyVault(vault).get(ref)
        assert vault.calls == []
    else:
        vault = FakeKeyVault({want: "v"})
        assert AzureKeyVault(vault).get(ref) == "v"
        assert vault.calls == [want]


# reads of one reference -> calls the vault sees
CACHE_CASES = [("once", 1, 1), ("twice", 2, 1), ("five times", 5, 1)]


@pytest.mark.parametrize(("case", "reads", "calls"), CACHE_CASES, ids=[c[0] for c in CACHE_CASES])
def test_key_vault_caches_per_run(case: str, reads: int, calls: int) -> None:
    """REL-05: a value is fetched once per run and held in memory only."""
    vault = FakeKeyVault({"a--b": "v"})
    store = AzureKeyVault(vault)
    assert [store.get("secret://a/b") for _ in range(reads)] == ["v"] * reads
    assert len(vault.calls) == calls


def test_key_vault_other_errors_are_not_hidden() -> None:
    """REL-05: only "not found" becomes SecretNotFound; any other failure (here a name the vault refuses) surfaces."""

    class Broken:
        def get_secret(self, name: str) -> Any:
            raise RuntimeError("vault unreachable")

    with pytest.raises(RuntimeError, match="unreachable"):
        AzureKeyVault(Broken()).get("secret://a")
