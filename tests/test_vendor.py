from __future__ import annotations

import dataclasses

import httpx
import pytest

from integration.handlers import Permanent
from integration.vendor import Bucket, Circuit, CircuitOpen, Disallowed, Unavailable, Vendor


class Fake:
    def __init__(self, *answers):
        self.answers, self.seen = list(answers), []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.seen.append(request)
        if request.url.path == "/oauth/token":
            return httpx.Response(200, json={"access_token": f"tok-{len(self.seen)}", "expires_in": 3600})
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    def calls(self):
        return [r for r in self.seen if r.url.path != "/oauth/token"]

    def tokens(self):
        return [r for r in self.seen if r.url.path == "/oauth/token"]


def vendor(cfg, secrets, fake, **over):
    out = dataclasses.replace(cfg.outbound, **over)
    pauses: list[float] = []
    v = Vendor(out, secrets, transport=httpx.MockTransport(fake), sleep=pauses.append, clock=lambda: 0.0)
    v.pauses = pauses  # type: ignore[attr-defined]
    return v


def test_calls_carry_a_cached_token_and_the_idempotency_key(cfg, secrets):
    fake = Fake(httpx.Response(201), httpx.Response(200))
    v = vendor(cfg, secrets, fake)
    v.post("/v1/orders", json={"a": 1}, idempotency_key="evt-1")
    v.get("/v1/orders/1", idempotency_key="evt-2")
    [token] = fake.tokens()
    assert token.url.host == "auth.example-labs.test"
    assert b"grant_type=client_credentials" in token.content and b"scope=orders.write" in token.content
    assert token.headers["authorization"].startswith("Basic ")
    first, second = fake.calls()
    assert first.headers["authorization"] == "Bearer tok-1" and first.headers["idempotency-key"] == "evt-1"
    assert second.headers["authorization"] == "Bearer tok-1"


def test_a_401_fetches_a_new_token_once(cfg, secrets):
    fake = Fake(httpx.Response(401), httpx.Response(200))
    vendor(cfg, secrets, fake).post("/v1/orders", idempotency_key="e")
    assert len(fake.tokens()) == 2
    assert fake.calls()[-1].headers["authorization"] == "Bearer tok-3"


def test_retries_with_backoff_and_retry_after_then_unavailable(cfg, secrets):
    fake = Fake(httpx.Response(503), httpx.Response(429, headers={"Retry-After": "7"}), httpx.Response(200))
    v = vendor(cfg, secrets, fake)
    assert v.post("/v1/orders", idempotency_key="e").status_code == 200
    assert v.pauses[1] == 7 and 0 <= v.pauses[0] <= 0.5
    fake = Fake(*[httpx.ConnectError("down")] * 3)
    v = vendor(cfg, secrets, fake, retries=2)
    with pytest.raises(Unavailable):
        v.post("/v1/orders", idempotency_key="e")
    assert len(fake.calls()) == 3


def test_4xx_and_redirects_are_permanent(cfg, secrets):
    with pytest.raises(Permanent, match="422"):
        vendor(cfg, secrets, Fake(httpx.Response(422))).post("/v1/orders", idempotency_key="e")
    redirect = httpx.Response(302, headers={"Location": "https://elsewhere.test/"})
    with pytest.raises(Permanent, match="redirect"):
        vendor(cfg, secrets, Fake(redirect)).post("/v1/orders", idempotency_key="e")


def test_only_allowed_hosts_over_https(cfg, secrets):
    v = vendor(cfg, secrets, Fake())
    for url in ("https://evil.test/x", "http://api.example-labs.test/x", "ftp://api.example-labs.test/x"):
        with pytest.raises(Disallowed):
            v.post(url, idempotency_key="e")
    assert v.url("//evil.test/x") == "https://api.example-labs.test/evil.test/x"  # a path, never a host


def test_the_circuit_opens_after_failures_and_closes_after_a_while(cfg, secrets):
    fake = Fake(*[httpx.Response(500)] * 2)
    v = vendor(cfg, secrets, fake, retries=0, circuit_failures=2)
    for _ in range(2):
        with pytest.raises(Unavailable):
            v.post("/v1/orders", idempotency_key="e")
    with pytest.raises(CircuitOpen):
        v.post("/v1/orders", idempotency_key="e")
    t = [0.0]
    c = Circuit(1, 60, clock=lambda: t[0])
    c.failure()
    assert c.is_open()
    t[0] = 61
    assert not c.is_open()
    c.success()
    assert c.failures == 0


def test_api_key_auth_and_the_rate_limit(cfg, secrets, secrets_dir):
    (secrets_dir / "lab-key").write_text("k-synthetic\n")
    fake = Fake(httpx.Response(200))
    v = vendor(cfg, secrets, fake, auth={"type": "api-key", "key": "lab-key", "header": "X-Api-Key"})
    v.put("/v1/x", idempotency_key="e")
    assert fake.calls()[0].headers["x-api-key"] == "k-synthetic" and not fake.tokens()
    t, slept = [0.0], []

    def sleep(s):
        slept.append(s)
        t[0] += s

    b = Bucket(2, clock=lambda: t[0], sleep=sleep)
    for _ in range(3):
        b.wait()
    assert slept == [pytest.approx(0.5)]
    v.close()


def test_a_token_endpoint_failure_is_unavailable(cfg, secrets):
    def broken(request):
        return httpx.Response(500)

    v = Vendor(cfg.outbound, secrets, transport=httpx.MockTransport(broken), sleep=lambda s: None)
    with pytest.raises(Unavailable, match="token"):
        v.post("/v1/orders", idempotency_key="e")
