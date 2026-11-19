"""Named world calendars (dwd.apply_calendar): the same incidents moved to other days, with the order of days kept."""

from pathlib import Path

import pytest

from dataplatform.world.dwd import DwdError, apply_calendar, load

SLICE = Path.home() / "data-platform-catalog/slices/02-tyre"
needs_world = pytest.mark.skipif(not SLICE.exists(), reason="needs ~/data-platform-catalog (the tyre DWD)")


def world(moves):
    return {"calendar": [{"id": "a", "day": 9, "params": {"from_day": 2, "activate_day": 11}},
                         {"id": "b", "day": 7, "params": {"until_day": 7}}],
            "calendars": {"short": {"moves": moves}}}


@pytest.mark.parametrize(
    ("case", "moves", "expect"),
    [
        ("days and params moved", {"a": {"day": 2, "from_day": 1, "activate_day": 3}, "b": {"day": 2, "until_day": 2}},
         [("a", 2, {"from_day": 1, "activate_day": 3}), ("b", 2, {"until_day": 2})]),
        ("a move without day keeps the day", {"a": {"from_day": 1}, "b": {}},
         [("a", 9, {"from_day": 1, "activate_day": 11}), ("b", 7, {"until_day": 7})]),
    ],
)
def test_calendar_moves(case, moves, expect):
    got = apply_calendar(world(moves), "short")
    assert [(e["id"], e["day"], e["params"]) for e in got["calendar"]] == expect
    assert got["calendar_name"] == "short"


@pytest.mark.parametrize(
    ("case", "name", "moves", "says"),
    [
        ("no such calendar", "other", {"a": {}, "b": {}}, "no calendar"),
        ("an incident left out", "short", {"a": {}}, "missing ['b']"),
        ("an unknown incident", "short", {"a": {}, "b": {}, "c": {}}, "unknown ['c']"),
        ("a key the incident does not have", "short", {"a": {"pick": 3}, "b": {}}, "not this incident's params"),
        ("source day not before the incident", "short", {"a": {"day": 2}, "b": {}}, "from_day"),
        ("effect not after the incident", "short", {"a": {"day": 11, "from_day": 1}, "b": {}}, "activate_day"),
        ("ends before it starts", "short", {"a": {}, "b": {"until_day": 6}}, "until_day"),
        ("day zero", "short", {"a": {}, "b": {"day": 0, "until_day": 0}}, "['day']"),
    ],
)
def test_calendar_refuses(case, name, moves, says):
    with pytest.raises(DwdError) as err:
        apply_calendar(world(moves), name)
    assert says in str(err.value)


def with_settings(sets):
    w = world({"a": {}, "b": {}})
    w["systems"] = {"drop": {"file_window_days": 5}}
    w["calendars"]["short"]["world"] = sets
    return w


@pytest.mark.parametrize(
    ("case", "sets", "says"),
    [
        ("an existing setting changes", {"systems.drop.file_window_days": 1}, ""),
        ("a path that is not there", {"systems.drop.window": 1}, "does not exist"),
        ("a parent that is not there", {"systems.nope.file_window_days": 1}, "does not exist"),
        ("a value of another type", {"systems.drop.file_window_days": "1"}, "is int, not str"),
    ],
)
def test_calendar_world_settings(case, sets, says):
    w = with_settings(sets)
    if says:
        with pytest.raises(DwdError) as err:
            apply_calendar(w, "short")
        assert says in str(err.value)
    else:
        assert apply_calendar(w, "short")["systems"]["drop"]["file_window_days"] == 1
        assert w["systems"]["drop"]["file_window_days"] == 5  # the world itself is not changed


@needs_world
def test_compressed_tyre_calendar_puts_every_incident_in_three_days():
    full, short = load(SLICE), load(SLICE, "compressed")
    assert sorted(e["id"] for e in short.world["calendar"]) == sorted(e["id"] for e in full.world["calendar"])
    assert max(e["day"] for e in short.world["calendar"]) <= short.world["calendars"]["compressed"]["days"] == 3
    assert min(e["day"] for e in short.world["calendar"]) == 2  # day 1 stays clean
    assert sorted(e["expect"]["detector"] for e in short.world["calendar"]) == \
        sorted(e["expect"]["detector"] for e in full.world["calendar"])


@needs_world
def test_the_default_is_the_worlds_own_calendar():
    assert max(e["day"] for e in load(SLICE).world["calendar"]) == 13
