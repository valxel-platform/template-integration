from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from integration import app as appmod
from integration.app import Runtime, build, create_app, metrics_body
from integration.settings import Settings
from integration.webhooks import sign

from .conftest import ROOT, SIGNING


@pytest.fixture
def settings(secrets_dir):
    return Settings(
        config_path=str(ROOT / "integration.yaml"),
        contracts_dir=str(ROOT / "contracts"),
        secrets_dir=str(secrets_dir),
        product="emr",
        audit_sink="memory",
    )


@pytest.fixture
def client(settings, store, auditor):
    rt = build(settings, store=store, auditor=auditor)
    return TestClient(create_app(settings, runtime=rt, workers=False))


def post(client, raw: bytes, stamp: int, **h):
    import time

    stamp = stamp or int(time.time())
    headers = {
        "X-Timestamp": str(stamp),
        "X-Signature": sign(SIGNING, str(stamp), raw),
        "X-Delivery-Id": "d-1",
        **h,
    }
    return client.post("/webhooks/lab-results", content=raw, headers=headers)


def test_a_signed_webhook_is_accepted_and_traced(client, store):
    raw = json.dumps({"order_ref": "O-1", "status": "final", "reported_at": "2026-11-17T10:00:00Z"}).encode()
    r = post(client, raw, 0)
    assert r.status_code == 202 and r.json() == {"status": "accepted"}
    assert len(store.claim()) == 1


def test_unsigned_stale_and_oversized_deliveries(client):
    raw = b"{}"
    assert client.post("/webhooks/lab-results", content=raw).status_code == 401
    assert post(client, raw, 1_000).status_code == 401
    big = b"x" * (256 * 1024 + 1)
    assert client.post("/webhooks/lab-results", content=big).status_code == 413


def test_only_declared_routes_exist_and_no_api_docs(client):
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").status_code == 200
    assert client.get("/docs").status_code == 404 and client.get("/openapi.json").status_code == 404
    assert client.get("/webhooks/lab-results").status_code == 405
    assert client.post("/webhooks/other").status_code == 404
    assert client.get("/metrics").status_code == 404  # on the management port (8081)
    assert "valxel_events" in metrics_body()[0].decode()
    assert client.post("/webhooks/lab-results", content=b"{}").headers["traceresponse"].startswith("00-")


def test_readyz_names_a_stopped_worker(settings, store, auditor):
    rt = build(settings, store=store, auditor=auditor)

    class Dead:
        name = "outbox-relay"

        def is_alive(self):
            return False

    rt.threads.append(Dead())
    c = TestClient(create_app(settings, runtime=rt, workers=False))
    assert c.get("/readyz").json() == {"status": "not ready", "stopped": ["outbox-relay"]}


def test_start_up_refuses_missing_secrets_and_plain_kafka_for_phi(settings, secrets_dir, store, auditor):
    (secrets_dir / "lab-client-secret").unlink()
    with pytest.raises(RuntimeError, match="lab-client-secret isn't mounted"):
        build(settings, store=store, auditor=auditor)
    (secrets_dir / "lab-client-secret").write_text("s\n")
    plain = settings.model_copy(update={"kafka_bootstrap": "kafka:9092"})
    with pytest.raises(RuntimeError, match="TLS"):
        build(plain, store=store, auditor=auditor)


def test_audit_is_required_for_phi_by_default(settings, store):
    rt = build(settings, store=store)
    assert isinstance(rt, Runtime) and rt.auditor.required and rt.auditor.data_class == "phi"


def test_without_kafka_workers_dont_start(settings, store, auditor, caplog):
    rt = build(settings, store=store, auditor=auditor)
    appmod.start_workers(rt)
    assert rt.threads == [] and "outbox" in caplog.text


def test_deployed_environments_need_the_database_and_kafka(settings, store, auditor):
    prod = settings.model_copy(update={"environment": "production"})
    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        build(prod, store=store, auditor=auditor)
