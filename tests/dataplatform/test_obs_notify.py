"""Contract table for the notifier port (ADR-024 rule 3): delivery through the platform's notification destinations.

The memory notifier and the destination notifier receive the same alerts; the destination one sends each to the
destination bound to the route's channel, with one payload shape. Routing and noise are decided before (obs/alerts).
"""

from datetime import UTC, datetime

import pytest

from dataplatform.adapters.databricks_notify import DestinationNotifier, UnroutedChannel, payload
from dataplatform.obs.alerts import Alert
from dataplatform.obs.monitor import MemoryNotifier

T = datetime(2026, 11, 9, 23, tzinfo=UTC)
ALERTS = [
    Alert("detector-fired", "2026-11-10", "page", "erp-rota", T,
          [{"subject": "erp.lines/DEL-01", "run_id": "a" * 32, "detail": "5 noticed (see sources_ingestion)"}]),
    Alert("freshness-breach", "2026-11-10", "digest", "sales@x", T,
          [{"subject": "partner.sellout/D-0042", "run_id": "", "detail": "bookmark_lag 744 h > 120 h"}]),
    Alert("detector-fired:resolved", "2026-11-11", "page", "erp-rota", T,
          [{"subject": "erp.lines/DEL-01", "run_id": "", "detail": "recovered"}]),
]
DESTINATIONS = {"page": "dest-pager", "ticket": "dest-queue", "digest": "dest-mail"}


def memory():
    n = MemoryNotifier()
    return n, lambda: [(a.channel, a.rule) for a in n.sent]


def destinations():
    posted = []
    n = DestinationNotifier(DESTINATIONS, lambda dest, body: posted.append((dest, body)))
    by_dest = {v: k for k, v in DESTINATIONS.items()}
    return n, lambda: [(by_dest[d], b["rule"]) for d, b in posted]


ADAPTERS = {"memory": memory, "destinations": destinations}


@pytest.mark.parametrize("adapter", list(ADAPTERS))
def test_every_alert_is_delivered_once_on_its_channel(adapter):
    notifier, delivered = ADAPTERS[adapter]()
    for a in ALERTS:
        notifier.send(a)
    assert delivered() == [(a.channel, a.rule) for a in ALERTS]


@pytest.mark.parametrize(
    ("case", "alert", "expect"),
    [
        ("an alert names rule, day and the run", ALERTS[0],
         ("Alert: detector-fired (2026-11-10)", ["erp.lines/DEL-01"], ["a" * 32])),
        ("a resolution says so", ALERTS[2], ("Resolved: detector-fired (2026-11-11)", ["erp.lines/DEL-01"], [])),
    ],
)
def test_payload(case, alert, expect):
    p = payload(alert)
    assert (p["title"], p["subjects"], p["run_ids"]) == expect


@pytest.mark.parametrize(
    ("case", "build"),
    [
        ("an unknown channel in the binding", lambda: DestinationNotifier({"sms": "x"}, lambda d, b: None)),
        ("a channel with no destination", lambda: DestinationNotifier({"page": "x"}, lambda d, b: None).send(
            ALERTS[1])),
    ],
)
def test_unrouted_is_refused_not_dropped(case, build):
    with pytest.raises(UnroutedChannel):
        build()
