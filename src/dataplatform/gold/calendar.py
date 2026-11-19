"""The date dimension (B15; gold modelling: one shared, role-playing date dimension). Pure and product-free.

No source sends a calendar, so the platform generates it: one row per day for a range, with calendar and fiscal
attributes. It is keyed by the date itself and by date_key = yyyymmdd, so a fact joins it on any of its date columns
(order, promised, delivered: the same table in different roles) with no change to the fact. Holidays are a list the
domain declares; nothing is guessed. Written to the shared gold schema (gold_shared__dim_date -> <env>_gold.shared).
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, timedelta
from typing import Any

TABLE = "gold_shared__dim_date"
MONTHS = ("January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
          "November", "December")
DAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
COLUMNS = (("date_key", "integer"), ("date", "date"), ("year", "integer"), ("quarter", "integer"),
           ("month", "integer"), ("month_name", "string"), ("year_month", "string"), ("iso_week", "integer"),
           ("iso_year", "integer"), ("day_of_month", "integer"), ("day_of_week", "integer"), ("day_name", "string"),
           ("is_weekend", "boolean"), ("is_holiday", "boolean"), ("is_working_day", "boolean"),
           ("fiscal_year", "integer"), ("fiscal_quarter", "integer"), ("fiscal_month", "integer"))


def fiscal(d: date, start_month: int) -> tuple[int, int, int]:
    """(fiscal year, fiscal quarter, fiscal month) for a fiscal year starting on the 1st of start_month; the fiscal
    year is named by the calendar year it ends in (India: April 2026 to March 2027 is fiscal year 2027)."""
    if not 1 <= start_month <= 12:  # noqa: PLR2004 -- months
        raise ValueError(f"fiscal start month {start_month} is not 1 to 12")
    shifted = (d.month - start_month) % 12
    year = d.year + (1 if start_month > 1 and d.month >= start_month else 0)
    return year, shifted // 3 + 1, shifted + 1


def calendar_rows(start: date, end: date, fiscal_start_month: int = 4,
                  holidays: Iterable[date] = (), weekend: tuple[int, ...] = (5, 6)) -> list[dict[str, Any]]:
    """One row per day from start to end inclusive. `weekend` holds weekday numbers (Monday = 0)."""
    if end < start:
        raise ValueError(f"calendar end {end} is before start {start}")
    off = set(holidays)
    rows = []
    d = start
    while d <= end:
        iso_year, iso_week, _ = d.isocalendar()
        fy, fq, fm = fiscal(d, fiscal_start_month)
        is_weekend, is_holiday = d.weekday() in weekend, d in off
        rows.append({
            "date_key": d.year * 10000 + d.month * 100 + d.day, "date": d, "year": d.year,
            "quarter": (d.month - 1) // 3 + 1, "month": d.month, "month_name": MONTHS[d.month - 1],
            "year_month": f"{d.year}-{d.month:02d}", "iso_week": iso_week, "iso_year": iso_year,
            "day_of_month": d.day, "day_of_week": d.weekday() + 1, "day_name": DAYS[d.weekday()],
            "is_weekend": is_weekend, "is_holiday": is_holiday, "is_working_day": not (is_weekend or is_holiday),
            "fiscal_year": fy, "fiscal_quarter": fq, "fiscal_month": fm,
        })
        d += timedelta(days=1)
    return rows
