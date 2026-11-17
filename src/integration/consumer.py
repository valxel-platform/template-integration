"""Outbound: product events this component reads, sent to the vendor.

One consumer (group <component>.outbound) over the topics of every dataset with an @outbound
handler. Per event:

  - read and checked against its contract: one that breaks it is parked on <topic>.dlt, naming
    the field, never its value;
  - skipped if it was already sent (the `sent` table, by event id);
  - sent by its handler through the vendor client, with the event id as Idempotency-Key;
  - recorded (integration.sent: actor this component, resource the dataset and event id) and
    marked sent; then its offset is committed.

The vendor refusing an event for good (Permanent) parks it too. The vendor being down
(Unavailable, CircuitOpen) pauses the consumer and retries the same event: nothing is skipped
while the vendor is away. Each event counts once for the flags it read (cohorts.item).
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable
from typing import Any

from opentelemetry import trace
from opentelemetry.propagate import extract

from . import cohorts, events
from .audit import Actor, Auditor, AuditUnavailable, Outcome
from .contracts import ContractError, Contracts, count
from .handlers import OUTBOUND, Permanent
from .store import Store
from .vendor import CircuitOpen, Unavailable

log = logging.getLogger("integration.consumer")
tracer = trace.get_tracer("integration")


class Outbound(threading.Thread):
    def __init__(
        self,
        store: Store,
        contracts: Contracts,
        vendor: Any,
        auditor: Auditor,
        consumer: Any,
        producer_factory: Callable[[], Any],
        *,
        component: str,
        pause_s: float = 5.0,
    ) -> None:
        super().__init__(daemon=True, name="outbound")
        self.store, self.contracts, self.vendor, self.auditor = store, contracts, vendor, auditor
        self.consumer, self.producer_factory, self.pause_s = consumer, producer_factory, pause_s
        self.actor = Actor(id=component, type="service")
        self.producer: Any = None
        self.stop = threading.Event()
        self.topics = sorted(c.topic for d in OUTBOUND if (c := contracts.for_dataset(d)))

    def run(self) -> None:
        if not self.topics:
            return
        self.consumer.subscribe(self.topics)
        try:
            while not self.stop.is_set():
                self.step()
        finally:
            self.consumer.close()

    def step(self) -> None:
        msg = self.consumer.poll(1.0)
        if msg is None:
            return
        if msg.error():
            log.warning("Kafka: %s", msg.error())
            return
        topic = msg.topic()
        contract = self.contracts.for_topic(topic)
        dataset = contract.dataset if contract else topic
        headers = {k: (v.decode() if isinstance(v, bytes) else str(v)) for k, v in (msg.headers() or [])}
        with tracer.start_as_current_span(
            "integration.send", context=extract(headers), attributes={"valxel.dataset": dataset}
        ):
            try:
                event = json.loads(msg.value())
                event_id = str(event["id"])
                if contract is None:
                    raise ContractError(dataset, "", "no contract")
                event["data"] = contract.check(event.get("data") or {})
            except ContractError as exc:
                self.park(msg, exc.dataset, exc.field, exc.rule)
                self.consumer.commit(message=msg, asynchronous=False)
                return
            except (ValueError, KeyError, TypeError) as exc:
                count(dataset, "skipped")
                log.error(
                    "%s: skipped an unreadable event at offset %s (%s)",
                    dataset,
                    msg.offset(),
                    type(exc).__name__,
                )
                self.consumer.commit(message=msg, asynchronous=False)
                return
            if self.store.was_sent(event_id):
                count(dataset, "skipped")
                self.consumer.commit(message=msg, asynchronous=False)
                return
            try:
                with cohorts.item():
                    OUTBOUND[dataset](event, self.vendor)
            except Permanent as exc:
                self.park(msg, dataset, "", "vendor-refused")
                try:
                    self._audit(dataset, event_id, "failure", str(exc))
                except AuditUnavailable:
                    log.error("%s: audit unavailable for refused event %s", dataset, event_id)
                self.consumer.commit(message=msg, asynchronous=False)
                return
            except (Unavailable, CircuitOpen) as exc:
                log.warning(
                    "%s: vendor unavailable (%s); retrying event %s", dataset, type(exc).__name__, event_id
                )
                self._retry(msg)
                return
            except Exception:
                log.exception("%s: handler failed for event %s; retrying", dataset, event_id)
                self._retry(msg)
                return
            try:
                self._audit(dataset, event_id, "success", "")
            except AuditUnavailable:
                # Sent but not recorded: try again; the vendor sees the same Idempotency-Key.
                log.error("%s: audit unavailable for event %s; retrying", dataset, event_id)
                self._retry(msg)
                return
            self.store.mark_sent(event_id, dataset)
            count(dataset, "sent")
            events.record("integration.sent", dataset=dataset)
            self.consumer.commit(message=msg, asynchronous=False)

    def _audit(self, dataset: str, event_id: str, outcome: Outcome, reason: str) -> None:
        self.auditor.record(
            "integration.sent",
            actor=self.actor,
            resource=(dataset, [event_id]),
            outcome=outcome,
            reason=reason,
        )

    def _retry(self, msg: Any) -> None:
        from confluent_kafka import TopicPartition

        self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
        self.stop.wait(self.pause_s)

    def park(self, msg: Any, dataset: str, field: str, rule: str) -> None:
        if self.producer is None:
            self.producer = self.producer_factory()
        self.producer.produce(
            f"{msg.topic()}.dlt",
            key=msg.key(),
            value=msg.value(),
            headers=[
                ("valxel-dataset", dataset.encode()),
                ("valxel-field", field.encode()),
                ("valxel-rule", rule.encode()),
                ("valxel-offset", str(msg.offset()).encode()),
            ],
        )
        self.producer.flush(10)
        count(dataset, "parked")
        log.error(
            "integration.event_parked dataset=%s field=%s rule=%s offset=%s",
            dataset,
            field,
            rule,
            msg.offset(),
        )
