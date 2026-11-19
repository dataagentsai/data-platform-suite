"""standardization group — universal data formats.

| Processor              | Default | What it does |
|------------------------|---------|--------------|
| email_validate         | on (when pii_class=email) | Format check; bad → NULL |
| email_split            | off (opt-in) | `<col>_local` + `<col>_domain` (1→N) |
| phone_normalize_e164   | off (opt-in) | "+1 (415) 555-1234" → "+14155551234" |
| country_code_iso       | off (opt-in) | "USA" / "U.S." → "US" (ISO Alpha-2) |
| currency_code_iso      | off (opt-in) | "$" / "dollar" → "USD" (ISO 4217) |
| url_canonicalize       | off (opt-in) | lowercase scheme + drop trailing / + sort query |

Most processors are off by default — standardization is opinionated;
customers opt in per column via ``transforms: [...]``. Only
``email_validate`` auto-fires (when ``pii_class=email``) because it's
strictly defensive (bad emails → NULL is safer than letting them flow).

The 1→N exception: ``email_split`` adds two new columns. It's the
only standardization that violates the strict 1:1 principle. Ship it
because the lineage benefit (clear local + domain split with named
outputs) outweighs the boundary erosion. Document the boundary so no
other 1→N processor sneaks in.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, ClassVar
from urllib.parse import urlsplit, urlunsplit

from lake_ingestion_framework.core.processors.hygiene import _apply_string_op
from lake_ingestion_framework.core.processors.types import (
    ColumnTransformContext,
    ProcessorGroup,
    ProcessorRunResult,
)

if TYPE_CHECKING:
    import pyarrow as pa


_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
_PHONE_NON_DIGIT_RE = re.compile(r"[^\d+]")
_STRING_TYPES = {"STRING", "VARCHAR", "TEXT", "CHAR", "NVARCHAR", "NCHAR"}


def _is_string_column(ctx: ColumnTransformContext) -> bool:
    bronze = (ctx.bronze_data_type or "").upper()
    silver = (ctx.silver_data_type or "").upper() if ctx.silver_data_type else ""
    return any(t in _STRING_TYPES for t in (bronze, silver) if t)


# ──────────────────────────────────────────────────────────────────────


class EmailValidate:
    """Reject malformed emails (bad → NULL). Auto-fires for
    ``pii_class=email`` columns; safe-by-default."""

    name = "email_validate"
    group = ProcessorGroup.STANDARDIZATION

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return ctx.pii_class == "email" and _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        def validate(s: str) -> str | None:
            return s if _EMAIL_RE.match(s) else None

        new_table, changed = _apply_string_op(table, ctx.silver_column_name, validate)
        return new_table, ProcessorRunResult(
            self.group,
            self.name,
            ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


class EmailSplit:
    """Adds ``_email_local`` + ``_email_domain`` derived from the
    column. **Only 1→N processor in the framework.** Opt-in via
    ``transforms: [email_split]``.

    Original email column is preserved. NULLs propagate.
    """

    name = "email_split"
    group = ProcessorGroup.STANDARDIZATION

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        import pyarrow as pa

        col = ctx.silver_column_name
        if col not in table.column_names:
            return table, ProcessorRunResult(self.group, self.name, col, fired=False)
        values = table.column(col).to_pylist()
        local: list[str | None] = []
        domain: list[str | None] = []
        for v in values:
            if v is None or "@" not in str(v):
                local.append(None)
                domain.append(None)
                continue
            head, _, tail = str(v).partition("@")
            local.append(head or None)
            domain.append(tail or None)
        new_table = table.append_column(
            f"{col}_local", pa.array(local, type=pa.string())
        ).append_column(f"{col}_domain", pa.array(domain, type=pa.string()))
        return new_table, ProcessorRunResult(
            self.group,
            self.name,
            col,
            fired=True,
            rows_affected=len(values),
        )


class PhoneNormalizeE164:
    """Strip non-digits except leading ``+``. Bad inputs (no digits)
    → NULL. Doesn't validate region codes — that's domain logic."""

    name = "phone_normalize_e164"
    group = ProcessorGroup.STANDARDIZATION

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        def normalize(s: str) -> str | None:
            stripped = _PHONE_NON_DIGIT_RE.sub("", s)
            if not stripped or stripped == "+":
                return None
            # Move all '+' to the front; drop interior pluses
            digits = stripped.replace("+", "")
            return ("+" if "+" in stripped else "") + digits if digits else None

        new_table, changed = _apply_string_op(table, ctx.silver_column_name, normalize)
        return new_table, ProcessorRunResult(
            self.group,
            self.name,
            ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


class CountryCodeIso:
    """Map common variants to ISO-3166-1 Alpha-2 ("US", "GB", ...).
    Only the most common variants ship; unknown values → NULL.
    Customers wanting full coverage maintain their own dimension table."""

    name = "country_code_iso"
    group = ProcessorGroup.STANDARDIZATION

    _VARIANTS: ClassVar[dict[str, str]] = {
        "US": "US",
        "USA": "US",
        "UNITED STATES": "US",
        "U.S.": "US",
        "U.S.A.": "US",
        "UK": "GB",
        "GB": "GB",
        "GREAT BRITAIN": "GB",
        "UNITED KINGDOM": "GB",
        "IN": "IN",
        "INDIA": "IN",
        "CA": "CA",
        "CANADA": "CA",
        "AU": "AU",
        "AUSTRALIA": "AU",
        "DE": "DE",
        "GERMANY": "DE",
        "FR": "FR",
        "FRANCE": "FR",
        "JP": "JP",
        "JAPAN": "JP",
        "CN": "CN",
        "CHINA": "CN",
        "BR": "BR",
        "BRAZIL": "BR",
        "MX": "MX",
        "MEXICO": "MX",
    }

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        variants = self._VARIANTS

        def to_iso(s: str) -> str | None:
            # Normalise: uppercase, strip whitespace, drop dots so
            # "U.S." / "U.S.A." both fold to "US" / "USA"
            return variants.get(s.upper().strip().replace(".", ""), None)

        new_table, changed = _apply_string_op(table, ctx.silver_column_name, to_iso)
        return new_table, ProcessorRunResult(
            self.group,
            self.name,
            ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


class CurrencyCodeIso:
    """Map common currency variants to ISO-4217 ("USD", "EUR", ...).
    Same posture as country codes — common-set only."""

    name = "currency_code_iso"
    group = ProcessorGroup.STANDARDIZATION

    _VARIANTS: ClassVar[dict[str, str]] = {
        "$": "USD",
        "USD": "USD",
        "DOLLAR": "USD",
        "DOLLARS": "USD",
        "US$": "USD",
        "€": "EUR",
        "EUR": "EUR",
        "EURO": "EUR",
        "EUROS": "EUR",
        "£": "GBP",
        "GBP": "GBP",
        "POUND": "GBP",
        "POUNDS": "GBP",
        "¥": "JPY",
        "JPY": "JPY",
        "YEN": "JPY",
        "₹": "INR",
        "INR": "INR",
        "RUPEE": "INR",
        "RUPEES": "INR",
        "C$": "CAD",
        "CAD": "CAD",
        "A$": "AUD",
        "AUD": "AUD",
        "CNY": "CNY",
        "RMB": "CNY",
        "YUAN": "CNY",
    }

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        variants = self._VARIANTS

        def to_iso(s: str) -> str | None:
            return variants.get(s.upper().strip(), None)

        new_table, changed = _apply_string_op(table, ctx.silver_column_name, to_iso)
        return new_table, ProcessorRunResult(
            self.group,
            self.name,
            ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


class UrlCanonicalize:
    """Lowercase scheme + host, drop trailing slash on path,
    drop default ports, sort query string. Bad URLs → NULL."""

    name = "url_canonicalize"
    group = ProcessorGroup.STANDARDIZATION

    def applies_to(self, ctx: ColumnTransformContext) -> bool:
        return _is_string_column(ctx)

    def run_arrow(
        self, table: pa.Table, ctx: ColumnTransformContext
    ) -> tuple[pa.Table, ProcessorRunResult]:
        def canonicalize(s: str) -> str | None:
            try:
                parts = urlsplit(s)
            except ValueError:
                return None
            if not parts.scheme or not parts.netloc:
                return None
            scheme = parts.scheme.lower()
            netloc = parts.netloc.lower()
            # drop default ports
            if (scheme == "http" and netloc.endswith(":80")) or (
                scheme == "https" and netloc.endswith(":443")
            ):
                netloc = netloc.rsplit(":", 1)[0]
            path = parts.path.rstrip("/") or ("/" if parts.path == "/" else "")
            # sort query params for canonical order
            query = "&".join(sorted(parts.query.split("&"))) if parts.query else ""
            return urlunsplit((scheme, netloc, path, query, ""))

        new_table, changed = _apply_string_op(table, ctx.silver_column_name, canonicalize)
        return new_table, ProcessorRunResult(
            self.group,
            self.name,
            ctx.silver_column_name,
            fired=changed > 0,
            rows_affected=changed,
        )


# Default-on processors fire automatically when applies_to returns True.
STANDARDIZATION_DEFAULT_ON = (EmailValidate(),)

# Opt-in processors require explicit ``transforms: [...]`` per column.
STANDARDIZATION_OPT_IN: dict[str, object] = {
    "email_split": EmailSplit(),
    "phone_normalize_e164": PhoneNormalizeE164(),
    "country_code_iso": CountryCodeIso(),
    "currency_code_iso": CurrencyCodeIso(),
    "url_canonicalize": UrlCanonicalize(),
}


__all__ = [
    "CountryCodeIso",
    "CurrencyCodeIso",
    "EmailSplit",
    "EmailValidate",
    "PhoneNormalizeE164",
    "STANDARDIZATION_DEFAULT_ON",
    "STANDARDIZATION_OPT_IN",
    "UrlCanonicalize",
]
