"""Wave 4 unit tests — standardization group."""

from __future__ import annotations

import pyarrow as pa
from lake_ingestion_framework.core.processors.standardization import (
    CountryCodeIso,
    CurrencyCodeIso,
    EmailSplit,
    EmailValidate,
    PhoneNormalizeE164,
    UrlCanonicalize,
)
from lake_ingestion_framework.core.processors.types import ColumnTransformContext


def _ctx(
    *,
    name: str = "value",
    pii_class: str = "none",
) -> ColumnTransformContext:
    return ColumnTransformContext(
        bronze_column_name=name,
        silver_column_name=name,
        bronze_data_type="VARCHAR",
        silver_data_type=None,
        cast_safety="strict",
        pii_class=pii_class,
        column_role=None,
        null_replacement=None,
    )


# ──────────────────────────────────────────────────────────────────────
# email_validate
# ──────────────────────────────────────────────────────────────────────


def test_email_validate_passes_valid_emails() -> None:
    table = pa.table({"value": pa.array(["alice@x.com", "bob@example.co.uk", "weird@a-b.io"])})
    new_table, result = EmailValidate().run_arrow(table, _ctx(pii_class="email"))
    assert result.fired is False  # no values changed
    assert new_table.column("value").to_pylist() == [
        "alice@x.com",
        "bob@example.co.uk",
        "weird@a-b.io",
    ]


def test_email_validate_nulls_invalid() -> None:
    table = pa.table({"value": pa.array(["bad-email", "no_at_sign", "alice@x.com"])})
    new_table, result = EmailValidate().run_arrow(table, _ctx(pii_class="email"))
    assert result.fired is True
    assert new_table.column("value").to_pylist() == [None, None, "alice@x.com"]


def test_email_validate_only_fires_for_pii_email() -> None:
    proc = EmailValidate()
    assert proc.applies_to(_ctx(pii_class="email")) is True
    assert proc.applies_to(_ctx(pii_class="none")) is False


# ──────────────────────────────────────────────────────────────────────
# email_split (1→N — the only one we ship)
# ──────────────────────────────────────────────────────────────────────


def test_email_split_adds_local_and_domain_columns() -> None:
    table = pa.table({"email": pa.array(["alice@x.com", "bob@example.co.uk", None])})
    new_table, result = EmailSplit().run_arrow(table, _ctx(name="email"))
    assert result.fired is True
    assert new_table.column("email_local").to_pylist() == ["alice", "bob", None]
    assert new_table.column("email_domain").to_pylist() == [
        "x.com",
        "example.co.uk",
        None,
    ]
    # original column preserved
    assert new_table.column("email").to_pylist() == [
        "alice@x.com",
        "bob@example.co.uk",
        None,
    ]


def test_email_split_handles_malformed_input() -> None:
    table = pa.table({"email": pa.array(["no-at-sign", "alice@x.com"])})
    new_table, _ = EmailSplit().run_arrow(table, _ctx(name="email"))
    assert new_table.column("email_local").to_pylist() == [None, "alice"]
    assert new_table.column("email_domain").to_pylist() == [None, "x.com"]


# ──────────────────────────────────────────────────────────────────────
# phone_normalize_e164
# ──────────────────────────────────────────────────────────────────────


def test_phone_normalize_strips_punctuation() -> None:
    table = pa.table(
        {"value": pa.array(["+1 (415) 555-1234", "415.555.1234", "(415) 555-1234", None])}
    )
    new_table, _ = PhoneNormalizeE164().run_arrow(table, _ctx())
    assert new_table.column("value").to_pylist() == [
        "+14155551234",
        "4155551234",
        "4155551234",
        None,
    ]


def test_phone_normalize_garbage_to_null() -> None:
    table = pa.table({"value": pa.array(["abc", "+", "()-"])})
    new_table, _ = PhoneNormalizeE164().run_arrow(table, _ctx())
    assert new_table.column("value").to_pylist() == [None, None, None]


# ──────────────────────────────────────────────────────────────────────
# country / currency code ISO mapping
# ──────────────────────────────────────────────────────────────────────


def test_country_code_iso_maps_common_variants() -> None:
    table = pa.table({"value": pa.array(["USA", "U.S.A.", "United Kingdom", "UK", "India"])})
    new_table, _ = CountryCodeIso().run_arrow(table, _ctx())
    assert new_table.column("value").to_pylist() == ["US", "US", "GB", "GB", "IN"]


def test_country_code_iso_unknown_to_null() -> None:
    table = pa.table({"value": pa.array(["Atlantis", "Mars"])})
    new_table, _ = CountryCodeIso().run_arrow(table, _ctx())
    assert new_table.column("value").to_pylist() == [None, None]


def test_currency_code_iso_maps_symbols_and_words() -> None:
    table = pa.table({"value": pa.array(["$", "USD", "Dollar", "€", "£"])})
    new_table, _ = CurrencyCodeIso().run_arrow(table, _ctx())
    assert new_table.column("value").to_pylist() == ["USD", "USD", "USD", "EUR", "GBP"]


# ──────────────────────────────────────────────────────────────────────
# url_canonicalize
# ──────────────────────────────────────────────────────────────────────


def test_url_canonicalize_lowercases_scheme_and_host() -> None:
    table = pa.table({"value": pa.array(["HTTPS://Example.COM/path", "https://x.io/"])})
    new_table, _ = UrlCanonicalize().run_arrow(table, _ctx())
    out = new_table.column("value").to_pylist()
    assert out[0] == "https://example.com/path"
    assert out[1] == "https://x.io/"


def test_url_canonicalize_drops_default_ports() -> None:
    table = pa.table(
        {"value": pa.array(["http://x.io:80/p", "https://x.io:443/p", "http://x.io:8080/p"])}
    )
    new_table, _ = UrlCanonicalize().run_arrow(table, _ctx())
    out = new_table.column("value").to_pylist()
    assert out[0] == "http://x.io/p"
    assert out[1] == "https://x.io/p"
    assert out[2] == "http://x.io:8080/p"  # non-default port preserved


def test_url_canonicalize_sorts_query_params() -> None:
    table = pa.table({"value": pa.array(["https://x.io/p?b=2&a=1&c=3"])})
    new_table, _ = UrlCanonicalize().run_arrow(table, _ctx())
    assert new_table.column("value").to_pylist()[0] == "https://x.io/p?a=1&b=2&c=3"


def test_url_canonicalize_garbage_to_null() -> None:
    table = pa.table({"value": pa.array(["not-a-url", "missing-scheme.com"])})
    new_table, _ = UrlCanonicalize().run_arrow(table, _ctx())
    assert new_table.column("value").to_pylist() == [None, None]
