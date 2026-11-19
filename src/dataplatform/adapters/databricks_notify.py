"""The notifier port (dataplatform.obs.monitor.Notifier) through the workspace's notification destinations.

Delivery is the platform's (ADR-024 rule 3): the workspace's notification destinations (Slack, PagerDuty, e-mail,
a webhook), configured once by the platform team. Routing by tier and the noise rules are ours (obs/alerts.py): this
adapter only picks the destination for the route's channel (page, ticket, digest) and formats the message.

`post(destination_id, payload)` is the transport: inside a job, the workspace client that sends to a destination.
It is injected so the contract table (tests/dataplatform/test_obs_notify.py) runs this adapter and the memory one
on the same alerts without a workspace; sending through a real workspace is an integration test (pending).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dataplatform.obs.alerts import CHANNELS, Alert

Post = Callable[[str, dict[str, Any]], None]


class UnroutedChannel(ValueError):
    """An alert's channel has no destination bound: refused at the first send, not dropped silently."""


def payload(alert: Alert) -> dict[str, Any]:
    subjects = [s["subject"] for s in alert.subjects]
    resolved = alert.rule.endswith(":resolved")
    title = f"{'Resolved' if resolved else 'Alert'}: {alert.rule.removesuffix(':resolved')} ({alert.day})"
    return {"title": title, "to": alert.to, "channel": alert.channel, "rule": alert.rule, "day": alert.day,
            "subjects": subjects, "details": [s.get("detail", "") for s in alert.subjects],
            "run_ids": sorted({s.get("run_id", "") for s in alert.subjects} - {""})}


class DestinationNotifier:
    def __init__(self, destinations: dict[str, str], post: Post) -> None:
        unknown = sorted(set(destinations) - set(CHANNELS))
        if unknown:
            raise UnroutedChannel(f"no such channel(s) {unknown}; channels are {CHANNELS}")
        self.destinations, self.post = dict(destinations), post

    def send(self, alert: Alert) -> None:
        dest = self.destinations.get(alert.channel)
        if not dest:
            raise UnroutedChannel(f"channel {alert.channel!r} has no notification destination bound")
        self.post(dest, payload(alert))
