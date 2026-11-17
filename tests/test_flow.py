"""The relay (outbox to Kafka) and the outbound consumer (Kafka to the vendor)."""

from __future__ import annotations

import pytest

from integration.consumer import Outbound
from integration.contracts import summary_line
from integration.relay import Relay
from integration.store import Outgoing
from integration.vendor import Unavailable

from .fakes import Consumer, Msg, Producer


def test_the_relay_publishes_and_marks_only_what_the_broker_acknowledged(store, contracts):
    store.accept(
        "lab-results",
        "d-1",
        "h",
        [Outgoing("emr.lab-result.v1", "ORD-1", {"id": "e1"}, {"valxel-dataset": "lab-result"})],
    )
    summary_line()
    broken = Relay(store, contracts, Producer(fail=True))
    with pytest.raises(RuntimeError, match="acknowledged"):
        broken.step()
    assert len(store.claim()) == 1
    producer = Producer()
    assert Relay(store, contracts, producer).step() == 1
    assert producer.sent[0]["topic"] == "emr.lab-result.v1" and producer.sent[0]["key"] == b"ORD-1"
    assert producer.sent[0]["headers"] == {"valxel-dataset": b"lab-result"}
    assert store.claim() == [] and Relay(store, contracts, producer).step() == 0
    assert summary_line() == 'valxel.datasets {"lab-result":{"published":1}}'


class Recorder:
    def __init__(self, fail=None):
        self.calls, self.fail = [], fail

    def post(self, path, *, idempotency_key, json):
        if self.fail:
            raise self.fail
        self.calls.append((path, idempotency_key, json))


def order(eid="evt-1", **data):
    return {
        "specversion": "1.0",
        "id": eid,
        "type": "lab-order",
        "data": {"order_id": "ORD-7", "tests": ["2345-7"], **data},
    }


def outbound(store, contracts, auditor, vendor, *messages):
    consumer, producer = Consumer(messages), Producer()
    out = Outbound(
        store,
        contracts,
        vendor,
        auditor,
        consumer,
        lambda: producer,
        component="template-integration",
        pause_s=0,
    )
    return out, consumer, producer


def test_an_order_is_sent_once_with_its_id_as_idempotency_key_and_recorded(store, contracts, auditor, sink):
    vendor = Recorder()
    out, consumer, _ = outbound(
        store,
        contracts,
        auditor,
        vendor,
        Msg("emr.lab-order.v1", order(extra="dropped")),
        Msg("emr.lab-order.v1", order(), 1),
    )
    assert out.topics == ["emr.lab-order.v1"]
    out.step()
    out.step()
    assert vendor.calls == [
        ("/v1/orders", "evt-1", {"order_ref": "ORD-7", "tests": [{"code": "2345-7"}], "priority": "routine"})
    ]
    assert consumer.committed == [0, 1]
    [rec] = sink.events
    assert rec["data"]["action"] == "integration.sent" and rec["data"]["resource"] == {
        "type": "lab-order",
        "ids": ["evt-1"],
    }


def test_contract_breaks_and_vendor_refusals_are_parked(store, contracts, auditor, sink):
    out, consumer, producer = outbound(
        store,
        contracts,
        auditor,
        Recorder(),
        Msg("emr.lab-order.v1", order(priority="whenever")),
        Msg("emr.lab-order.v1", order("evt-2", tests=[]), 1),
        Msg("emr.lab-order.v1", b"not json", 2),
    )
    out.step()
    out.step()
    out.step()
    assert [p["topic"] for p in producer.sent] == ["emr.lab-order.v1.dlt"] * 2
    assert producer.sent[0]["headers"]["valxel-field"] == b"priority"
    assert producer.sent[1]["headers"]["valxel-rule"] == b"vendor-refused"
    assert consumer.committed == [0, 1, 2]
    assert sink.events[0]["data"]["outcome"] == "failure"


def test_while_the_vendor_is_down_the_same_event_is_retried(store, contracts, auditor):
    vendor = Recorder(fail=Unavailable("down"))
    out, consumer, _ = outbound(store, contracts, auditor, vendor, Msg("emr.lab-order.v1", order(), 5))
    out.step()
    assert consumer.committed == [] and consumer.seeks == [5]
    vendor.fail = None
    out.step()
    assert consumer.committed == [5]


def test_no_audit_record_means_retry(store, contracts, auditor, sink):
    sink.fail = True
    out, consumer, _ = outbound(store, contracts, auditor, Recorder(), Msg("emr.lab-order.v1", order(), 3))
    out.step()
    assert consumer.seeks == [3] and not store.was_sent("evt-1")
