"""The generated date dimension (gold/calendar.py, B15)."""

from datetime import date

import pytest

from dataplatform.gold.calendar import COLUMNS, calendar_rows, fiscal


@pytest.mark.parametrize(
    ("case", "day", "start", "want"),
    [
        ("India: April opens fiscal year 2027", date(2026, 4, 1), 4, (2027, 1, 1)),
        ("India: March closes fiscal year 2026", date(2026, 3, 31), 4, (2026, 4, 12)),
        ("India: November is quarter 3", date(2026, 11, 14), 4, (2027, 3, 8)),
        ("calendar fiscal year", date(2026, 11, 14), 1, (2026, 4, 11)),
        ("July fiscal year", date(2026, 7, 1), 7, (2027, 1, 1)),
    ],
)
def test_fiscal(case, day, start, want):
    assert fiscal(day, start) == want


def test_fiscal_start_must_be_a_month():
    with pytest.raises(ValueError, match="not 1 to 12"):
        fiscal(date(2026, 1, 1), 13)


def test_calendar_rows():
    rows = calendar_rows(date(2026, 11, 13), date(2026, 11, 16), holidays=[date(2026, 11, 16)])
    assert [r["date_key"] for r in rows] == [20261113, 20261114, 20261115, 20261116]
    assert [r["day_name"] for r in rows] == ["Friday", "Saturday", "Sunday", "Monday"]
    assert [r["is_working_day"] for r in rows] == [True, False, False, False]  # weekend, then a holiday
    assert rows[0]["year_month"] == "2026-11"
    assert rows[0]["iso_week"] == 46
    assert list(rows[0]) == [c for c, _ in COLUMNS]


def test_a_leap_year_has_366_days():
    assert len(calendar_rows(date(2028, 1, 1), date(2028, 12, 31))) == 366


def test_end_before_start_is_refused():
    with pytest.raises(ValueError, match="before start"):
        calendar_rows(date(2026, 2, 1), date(2026, 1, 1))
