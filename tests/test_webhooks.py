from __future__ import annotations

import json
import time

import pytest

from integration.webhooks import RateLimit, Receiver, sign, signature_ok

from .conftest import SIGNING

NOW = 1_800_000_000


def body(**over):
    return json.dumps(
        {
            "order_ref": "ORD-1001",
            "status": "final",
            "loinc": "2345-7",
            "reported_at": "2026-11-17T10:00:00Z",
            **over,
        }
    ).encode()


def headers(raw: bytes, *, delivery="d-1", stamp=NOW, secret=SIGNING, prefix="sha256="):
    return {
        "X-Timestamp": str(stamp),
        "X-Signature": prefix + sign(secret, str(stamp), raw),
        "X-Delivery-Id": delivery,
    }


@pytest.fixture
def receiver(cfg, secrets, store, contracts, auditor):
    return Receiver(
        cfg, secrets, store, contracts, auditor, source="emr/template-integration", clock=lambda: NOW
    )


def test_a_signed_delivery_is_accepted_once_and_its_event_goes_to_the_outbox(receiver, cfg, store, sink):
    hook = cfg.webhooks[0]
    raw = body()
    a = receiver.receive(hook, headers(raw), raw)
    assert (a.status, a.body) == (202, {"status": "accepted"})
    assert receiver.receive(hook, headers(raw), raw).body == {"status": "already received"}
    [q] = store.claim()
    assert q.topic == "emr.lab-result.v1" and q.key == "ORD-1001"
    assert q.value["type"] == "lab-result" and q.value["dataschema"] == "lab-result@1.0.0"
    assert q.value["data"] == {
        "order_id": "ORD-1001",
        "status": "final",
        "result_code": "2345-7",
        "reported_at": "2026-11-17T10:00:00Z",
    }
    assert q.headers == {"valxel-dataset": "lab-result", "valxel-delivery": "d-1"}
    [rec] = sink.events
    assert rec["data"]["action"] == "integration.received"
    assert rec["data"]["actor"] == {"id": "example-labs", "type": "service", "client": ""}
    assert rec["data"]["resource"] == {"type": "webhook-delivery", "ids": ["lab-results/d-1"]}
    assert "ORD-1001" not in json.dumps(rec)  # ids of deliveries, never the record's values
    assert store.inbox[("lab-results", "d-1")]["sha256"] and "body" not in store.inbox[("lab-results", "d-1")]


@pytest.mark.parametrize(
    "change",
    [
        {"secret": "wrong"},
        {"stamp": NOW - 301},
        {"stamp": NOW + 301},
        {"prefix": "sha1="},
    ],
)
def test_bad_signatures_and_replays_get_a_bare_401(receiver, cfg, store, sink, change, caplog):
    raw = body()
    a = receiver.receive(cfg.webhooks[0], headers(raw, **change), raw)
    assert (a.status, a.body) == (401, {"error": "unauthorized"})
    assert store.claim() == []
    assert sink.events[0]["data"]["outcome"] == "denied"
    assert "integration.signature_rejected webhook=lab-results" in caplog.text  # the alert rule's marker


def test_a_tampered_body_or_missing_headers_are_refused(receiver, cfg):
    raw = body()
    h = headers(raw)
    assert receiver.receive(cfg.webhooks[0], h, body(status="preliminary")).status == 401
    assert receiver.receive(cfg.webhooks[0], {}, raw).status == 401
    no_id = {k: v for k, v in h.items() if k != "X-Delivery-Id"}
    assert receiver.receive(cfg.webhooks[0], no_id, raw).status == 400


def test_the_previous_secret_still_works_during_a_rotation(receiver, cfg, secrets_dir):
    (secrets_dir / "lab-results-signing").write_text("whsec-next\n" + SIGNING + "\n")
    import os

    os.utime(secrets_dir / "lab-results-signing", (5, 5))
    raw = body()
    assert receiver.receive(cfg.webhooks[0], headers(raw), raw).status == 202
    assert (
        receiver.receive(cfg.webhooks[0], headers(raw, delivery="d-2", secret="whsec-next"), raw).status
        == 202
    )


def test_data_that_breaks_the_contract_is_kept_as_failed_and_not_published(
    receiver, cfg, store, sink, caplog
):
    raw = body(order_ref="x" * 65)
    a = receiver.receive(cfg.webhooks[0], headers(raw), raw)
    assert a.status == 422 and a.body["field"] == "order_id"
    assert store.inbox[("lab-results", "d-1")]["status"] == "failed"
    assert "x" * 65 not in store.inbox[("lab-results", "d-1")]["error"]
    assert store.claim() == []
    assert "integration.delivery_refused webhook=lab-results" in caplog.text
    assert sink.events[-1]["data"]["outcome"] == "failure"
    assert (
        receiver.receive(cfg.webhooks[0], headers(raw), raw).status == 422
    )  # again: same answer, one record
    assert len(sink.events) == 1


def test_unreadable_bodies_are_400_after_the_signature(receiver, cfg):
    for raw in (b"not json", b"[1]", body(status="weird")):
        assert receiver.receive(cfg.webhooks[0], headers(raw), raw).status == 400


def test_too_large_and_rate_limited(receiver, cfg):
    raw = b"x" * (cfg.webhooks[0].max_body_kb * 1024 + 1)
    assert receiver.receive(cfg.webhooks[0], headers(raw), raw).status == 413
    t = [0.0]
    limit = RateLimit(2, clock=lambda: t[0])
    assert [limit.take(), limit.take()] == [0.0, 0.0]
    assert limit.take() == pytest.approx(30.0)
    t[0] = 30.0
    assert limit.take() == 0.0
    receiver.limits["lab-results"] = RateLimit(1, clock=lambda: 0.0)
    assert receiver.receive(cfg.webhooks[0], headers(body()), body()).status == 202
    a = receiver.receive(cfg.webhooks[0], headers(body(), delivery="d-9"), body())
    assert a.status == 429 and a.headers == {"Retry-After": "60"}


def test_no_audit_record_means_no_acceptance(receiver, cfg, store, sink):
    sink.fail = True
    raw = body()
    assert receiver.receive(cfg.webhooks[0], headers(raw), raw).status == 503
    assert store.claim() == [] and not store.seen("lab-results", "d-1")
    sink.fail = False
    assert receiver.receive(cfg.webhooks[0], headers(raw), raw).status == 202  # the vendor's retry


def test_an_unmounted_signing_secret_refuses_everything(receiver, cfg, secrets_dir):
    (secrets_dir / "lab-results-signing").unlink()
    raw = body()
    assert receiver.receive(cfg.webhooks[0], headers(raw), raw).status == 503


def test_signature_comparison():
    raw = b"{}"
    good = sign("a", "1", raw)
    assert signature_ok(["b", "a"], "1", raw, good.upper())
    assert not signature_ok(["b"], "1", raw, good)
    assert time.time() > 0


def test_the_example_feature_is_behind_its_flag(receiver, cfg):
    """The pattern every feature follows: its new behaviour reads its flag, and off is today's."""
    from openfeature.provider.in_memory_provider import InMemoryFlag, InMemoryProvider

    from integration import flags
    from integration.webhooks import example_flag

    assert example_flag("emr") == "emr.example.receipt-names-delivery"
    assert example_flag("") == "example.example.receipt-names-delivery"
    hook, raw = cfg.webhooks[0], body()
    assert receiver.receive(hook, headers(raw, delivery="d-off"), raw).body == {"status": "accepted"}
    on = InMemoryFlag("on", {"on": True, "off": False})
    flags.use_provider(InMemoryProvider({example_flag("emr"): on}))
    try:
        raw = body(order_ref="ORD-1002")
        a = receiver.receive(hook, headers(raw, delivery="d-on"), raw)
        assert a.body == {"status": "accepted", "delivery": "d-on"}
    finally:
        flags.use_provider(InMemoryProvider({}))
