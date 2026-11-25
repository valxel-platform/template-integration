"""The mapping between the vendor's shapes and the product's events: the code the coder writes.

    from integration.handlers import Emit, inbound, outbound

    @inbound("lab-results")                       # a webhook in integration.yaml
    def lab_result(body: dict) -> list[Emit]:
        return [Emit("lab-result", key=body["order_id"],
                     data={"order_id": body["order_id"], "status": body["status"]})]

    @outbound("lab-order")                        # a dataset this component reads
    def send_order(event: dict, vendor: Vendor) -> None:
        vendor.post("/orders", json={"ref": event["data"]["order_id"]}, idempotency_key=event["id"])

    @pull("test-catalog")                         # a pull source in integration.yaml
    def catalog_item(record: dict) -> list[Emit | Delete]:
        if record.get("retired"):
            return [Delete("lab-test", key=record["code"])]
        return [Emit("lab-test", key=record["code"], data={"code": record["code"], ...})]

An inbound handler turns one verified delivery into product events; each is checked against its
dataset's contract before it's published. An outbound handler sends one product event to the
vendor, through the client that enforces the allowed hosts, auth, limits and circuit breaker.
Raise Permanent for an event the vendor will never accept (it's parked, not retried).

A pull handler turns one record read from a source (pull.py fetches, pages and keeps the cursor)
into product events, or into Delete for a record the source says is gone: published as
<dataset>.deleted with only the key, so consumers remove it. A record that breaks its contract is
parked (its key and the field, never the value) and the sync carries on.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Emit:
    dataset: str
    key: str
    data: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Delete:
    """The record is gone at the source: consumers remove it (a <dataset>.deleted event)."""

    dataset: str
    key: str


class Permanent(Exception):
    """The vendor refused this event for good (a 4xx other than 408, 409 and 429): park it."""


Inbound = Callable[[dict[str, Any]], list[Emit]]
Outbound = Callable[[dict[str, Any], Any], None]
Pull = Callable[[dict[str, Any]], list["Emit | Delete"]]

INBOUND: dict[str, Inbound] = {}
OUTBOUND: dict[str, Outbound] = {}
PULL: dict[str, Pull] = {}


def inbound(webhook: str) -> Callable[[Inbound], Inbound]:
    def register(fn: Inbound) -> Inbound:
        if webhook in INBOUND:
            raise ValueError(f"webhook {webhook} already has a handler")
        INBOUND[webhook] = fn
        return fn

    return register


def outbound(dataset: str) -> Callable[[Outbound], Outbound]:
    def register(fn: Outbound) -> Outbound:
        if dataset in OUTBOUND:
            raise ValueError(f"dataset {dataset} already has a handler")
        OUTBOUND[dataset] = fn
        return fn

    return register


def pull(source: str) -> Callable[[Pull], Pull]:
    def register(fn: Pull) -> Pull:
        if source in PULL:
            raise ValueError(f"pull source {source} already has a handler")
        PULL[source] = fn
        return fn

    return register


def check(webhooks: list[str], datasets: list[str], pulls: list[str] | None = None) -> list[str]:
    """Start-up check: every webhook and pull source has a handler, every handler a webhook, a pull
    source or a contract."""
    pulls = pulls or []
    problems = [f"webhook {w} has no @inbound handler" for w in webhooks if w not in INBOUND]
    problems += [f"@inbound({w}) names no webhook in integration.yaml" for w in INBOUND if w not in webhooks]
    problems += [f"pull source {p} has no @pull handler" for p in pulls if p not in PULL]
    problems += [f"@pull({p}) names no pull source in integration.yaml" for p in PULL if p not in pulls]
    problems += [
        f"@outbound({d}) names a dataset with no contract in contracts/"
        for d in OUTBOUND
        if d not in datasets
    ]
    return problems
