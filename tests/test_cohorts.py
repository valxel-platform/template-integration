"""Requests per flag, on and off, for the SDLC tool's gradual releases."""

import json
import logging

import pytest
from openfeature.provider.in_memory_provider import InMemoryFlag, InMemoryProvider
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from integration import cohorts, flags

KEY = "emr.emr-feat-3.referral-lookup"


def on(value: str = "on") -> None:
    flags.use_provider(InMemoryProvider({KEY: InMemoryFlag(value, {"on": True, "off": False})}))


async def ok(request):
    flags.enabled(KEY, user="clinic-1")
    flags.enabled(KEY, user="clinic-1")  # twice in one delivery: still one
    return JSONResponse({"status": "accepted"}, status_code=202)


async def refused(request):
    flags.enabled(KEY)
    return JSONResponse({"error": "unauthorized"}, status_code=401)  # the caller's fault: not failed


async def unavailable(request):
    flags.enabled(KEY)
    return JSONResponse({"error": "unavailable"}, status_code=503)


async def boom(request):
    flags.enabled(KEY)
    raise RuntimeError("boom")


async def plain(request):
    return JSONResponse({}, status_code=500)  # no flag evaluated: not counted


@pytest.fixture
def client():
    cohorts.drain()
    app = Starlette(
        routes=[
            Route(p, f, methods=["POST"])
            for p, f in (
                ("/ok", ok),
                ("/refused", refused),
                ("/unavailable", unavailable),
                ("/boom", boom),
                ("/plain", plain),
            )
        ]
    )
    yield TestClient(cohorts.CohortMiddleware(app), raise_server_exceptions=False)
    flags.use_provider(InMemoryProvider({}))


def test_deliveries_count_once_per_flag_and_5xx_or_exceptions_are_failed(client):
    on()
    assert client.post("/ok").status_code == 202
    assert client.post("/refused").status_code == 401
    assert client.post("/unavailable").status_code == 503
    assert client.post("/boom").status_code == 500
    client.post("/plain")
    on("off")
    client.post("/ok")
    [line] = cohorts.drain()
    assert json.loads(line[len(cohorts.MARKER) :]) == {
        "flag": KEY,
        "on": {"requests": 4, "errors": 2, "slow": 0},
        "off": {"requests": 1, "errors": 0, "slow": 0},
    }


def test_an_event_sent_on_counts_as_one_item_and_failed_when_it_raises():
    cohorts.drain()
    on()
    with cohorts.item():
        flags.enabled(KEY)
    with pytest.raises(RuntimeError), cohorts.item():
        flags.enabled(KEY)
        raise RuntimeError("vendor down")
    [line] = cohorts.drain()
    assert json.loads(line[len(cohorts.MARKER) :])["on"] == {"requests": 2, "errors": 1, "slow": 0}
    flags.use_provider(InMemoryProvider({}))


def test_reports_are_logged_and_outside_a_request_nothing_counts(caplog):
    on()
    flags.enabled(KEY)
    assert cohorts.drain() == []
    token = cohorts.begin()
    cohorts.seen(KEY, False)
    cohorts.seen("not a key!", True)
    cohorts.end(token, failed=True, elapsed_ms=cohorts.SLOW_MS + 1)
    with caplog.at_level(logging.INFO, logger="integration.cohorts"):
        cohorts.report()
    assert [r.getMessage() for r in caplog.records] == [
        cohorts.MARKER
        + ' {"flag":"'
        + KEY
        + '","on":{"requests":0,"errors":0,"slow":0},"off":{"requests":1,"errors":1,"slow":1}}'
    ]
    cohorts.start_reporting(period_s=3600)
    cohorts.start_reporting(period_s=3600)
    flags.use_provider(InMemoryProvider({}))
