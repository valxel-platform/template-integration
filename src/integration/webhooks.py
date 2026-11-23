"""Webhooks: the only way in from outside.

Each delivery is checked, in this order, before anything reads it:

  size        Content-Length and the bytes read are capped (max_body_kb): 413.
  rate        per webhook, per replica (rate_per_minute): 429 with Retry-After.
  timestamp   the timestamp header must be within tolerance_s of now: older ones are replays.
  signature   HMAC-SHA256 over "<timestamp>.<raw body>" with the webhook's secret (the current or,
              during a rotation, the previous value), compared in constant time. The header may
              carry "sha256=" before the hex digest.
  delivery id required: a delivery seen before is answered as before and never published twice.

Any failure of the timestamp or signature is a bare 401: the caller learns nothing about which
check failed. Only then is the body parsed and mapped (handlers.py); the events it maps to are
checked against their contracts and written to the outbox in the same transaction as the inbox
row, then 202. A delivery whose events break a contract is kept as failed (422) and counted.

Audit: integration.received (actor: the vendor, resource: the webhook and delivery id; ids only)
for every accepted, refused or rejected delivery, so who sent what is answerable later.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from . import events, flags
from .audit import Actor, Auditor, AuditUnavailable, Outcome
from .config import Config, Webhook
from .contracts import ContractError, Contracts, count
from .handlers import INBOUND
from .secrets import SecretMissing, Secrets
from .store import Outgoing, Store

log = logging.getLogger("integration.webhooks")


@dataclass(frozen=True)
class Answer:
    status: int
    body: dict[str, Any]
    headers: dict[str, str] | None = None


UNAUTHORIZED = Answer(401, {"error": "unauthorized"})


def example_flag(product: str) -> str:
    """The example feature's flag. It shows the pattern every feature follows: the tool gives the
    feature a flag in the product's Flipt (on in dev and staging, off in production until it's
    released) and the new behaviour reads it. Replace it with your first feature's own."""
    return f"{product or 'example'}.example.receipt-names-delivery"


def sign(secret: str, timestamp: str, body: bytes) -> str:
    """The signature a vendor sends (tests and the post-deploy smoke use it too)."""
    return hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()


def signature_ok(secrets: list[str], timestamp: str, body: bytes, header: str) -> bool:
    given = header.strip()
    if given.lower().startswith("sha256="):
        given = given[7:]
    ok = False
    for secret in secrets:  # every value is compared, so timing doesn't tell which matched
        ok |= hmac.compare_digest(sign(secret, timestamp, body), given.lower())
    return ok


class RateLimit:
    """A token bucket per webhook, refilled continuously to rate_per_minute."""

    def __init__(self, per_minute: int, clock: Callable[[], float] = time.monotonic) -> None:
        self.capacity, self.rate, self.clock = float(per_minute), per_minute / 60.0, clock
        self.tokens, self.at = float(per_minute), clock()
        self.lock = threading.Lock()

    def take(self) -> float:
        """0 when allowed, else seconds until a token is free."""
        with self.lock:
            now = self.clock()
            self.tokens = min(self.capacity, self.tokens + (now - self.at) * self.rate)
            self.at = now
            if self.tokens >= 1:
                self.tokens -= 1
                return 0.0
            return (1 - self.tokens) / self.rate


class Receiver:
    def __init__(
        self,
        cfg: Config,
        secrets: Secrets,
        store: Store,
        contracts: Contracts,
        auditor: Auditor,
        *,
        source: str,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.cfg, self.secrets, self.store, self.contracts, self.auditor = (
            cfg,
            secrets,
            store,
            contracts,
            auditor,
        )
        self.source, self.clock = source, clock
        self.limits = {w.name: RateLimit(w.rate_per_minute) for w in cfg.webhooks}
        self.vendor = Actor(id=cfg.vendor, type="service")

    def _audit(
        self, hook: Webhook, delivery: str, outcome: Outcome, reason: str, details: dict[str, Any]
    ) -> None:
        ids = [f"{hook.name}/{delivery}" if delivery else hook.name]
        self.auditor.record(
            "integration.received",
            actor=self.vendor,
            resource=("webhook-delivery", ids),
            outcome=outcome,
            reason=reason,
            details={"webhook": hook.name, **details},
        )

    def receive(self, hook: Webhook, headers: dict[str, str], body: bytes) -> Answer:
        h = {k.lower(): v for k, v in headers.items()}
        wait = self.limits[hook.name].take()
        if wait:
            return Answer(429, {"error": "rate limited"}, {"Retry-After": str(max(1, int(wait + 0.999)))})
        if len(body) > hook.max_body_kb * 1024:
            return Answer(413, {"error": "too large"})
        timestamp, signature = h.get(hook.timestamp_header.lower(), ""), h.get(hook.header.lower(), "")
        delivery = h.get(hook.delivery_id_header.lower(), "")[:200]
        try:
            stamp_ok = timestamp.isdigit() and abs(self.clock() - int(timestamp)) <= hook.tolerance_s
            sig_ok = bool(signature) and signature_ok(
                self.secrets.values(hook.secret), timestamp, body, signature
            )
        except SecretMissing:
            log.error("webhook %s: its signing secret isn't mounted; refusing deliveries", hook.name)
            return Answer(503, {"error": "unavailable"})
        if not (stamp_ok and sig_ok):
            events.record("integration.webhook_rejected", webhook=hook.name)
            log.warning(
                "integration.signature_rejected webhook=%s check=%s",
                hook.name,
                "signature" if stamp_ok else "timestamp",
            )
            try:
                self._audit(hook, "", "denied", "signature" if stamp_ok else "timestamp", {})
            except AuditUnavailable:
                log.error("webhook %s: audit unavailable for a rejected delivery", hook.name)
            return UNAUTHORIZED
        if not delivery:
            return Answer(400, {"error": f"{hook.delivery_id_header} is required"})
        digest = hashlib.sha256(body).hexdigest()
        try:
            payload = json.loads(body)
            if not isinstance(payload, dict):
                raise ValueError("not an object")
            emitted = INBOUND[hook.name](payload)
            outgoing = [self._event(e.dataset, e.key, e.data, delivery) for e in emitted]
        except ContractError as exc:
            count(exc.dataset, "refused")
            if self.store.refuse(hook.name, delivery, digest, str(exc)):
                try:
                    self._audit(
                        hook, delivery, "failure", "contract", {"dataset": exc.dataset, "field": exc.field}
                    )
                except AuditUnavailable:
                    log.error("webhook %s: audit unavailable for refused delivery %s", hook.name, delivery)
            log.error("integration.delivery_refused webhook=%s delivery=%s: %s", hook.name, delivery, exc)
            return Answer(422, {"error": "the data doesn't match the agreed contract", "field": exc.field})
        except (ValueError, KeyError, TypeError) as exc:
            log.error(
                "webhook %s: delivery %s couldn't be read (%s)", hook.name, delivery, type(exc).__name__
            )
            return Answer(400, {"error": "unreadable"})
        if self.store.seen(hook.name, delivery):
            return Answer(202, {"status": "already received"})
        try:
            # The record comes first: a delivery is never in the outbox without it. If the store then
            # fails, the vendor retries and the retry is recorded again (twice, never zero times).
            self._audit(hook, delivery, "success", "", {"events": len(outgoing)})
        except AuditUnavailable:
            log.error("webhook %s: audit unavailable; delivery %s not accepted", hook.name, delivery)
            return Answer(503, {"error": "unavailable"})
        if not self.store.accept(hook.name, delivery, digest, outgoing):
            return Answer(202, {"status": "already received"})
        events.record("integration.webhook_accepted", webhook=hook.name)
        # The template's example feature, behind its flag: off, the receipt is today's. On, it
        # names the delivery it accepted, so the vendor can match it to what it sent.
        if flags.enabled(example_flag(self.source.split("/", 1)[0]), user=self.cfg.vendor):
            return Answer(202, {"status": "accepted", "delivery": delivery})
        return Answer(202, {"status": "accepted"})

    def _event(self, dataset: str, key: str, data: dict[str, Any], delivery: str) -> Outgoing:
        contract = self.contracts.for_dataset(dataset)
        if contract is None:
            raise ContractError(dataset, "", "no contract: add the dataset to the design")
        checked = contract.check_whole(data)
        value = {
            "specversion": "1.0",
            "id": str(uuid.uuid4()),
            "source": self.source,
            "type": dataset,
            "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.clock())),
            "datacontenttype": "application/json",
            "dataschema": f"{dataset}@{contract.version}",
            "data": checked,
        }
        return Outgoing(
            topic=contract.topic,
            key=str(key),
            value=value,
            headers={"valxel-dataset": dataset, "valxel-delivery": delivery},
        )
