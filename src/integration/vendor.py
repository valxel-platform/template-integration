"""The client for calls to the vendor: the only way out.

Every request:
  - goes to an allowed host over https (integration.yaml's allowed_hosts), never follows a
    redirect, and never carries credentials anywhere else;
  - is authenticated as integration.yaml says: an OAuth 2.0 client-credentials token (cached
    until a minute before it expires, fetched again on a 401), an API key header, or a client
    certificate (mTLS); secrets come from mounted files (secrets.py), never logged;
  - waits for the rate limit (rate_per_s, per replica);
  - carries an Idempotency-Key, so a retry is never a second order at the vendor;
  - is retried on connection errors, 408, 429 and 5xx, up to `retries` times with exponential
    backoff and full jitter, honouring Retry-After (capped at 60 s);
  - fails fast while the circuit is open: after `circuit.failures` failures in a row, calls stop for
    `circuit.open_s`, then one is let through to see whether the vendor is back.

Any other 4xx is Permanent: the event is parked, not retried. Responses' bodies are never logged.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx

from .config import Outbound
from .handlers import Permanent
from .secrets import Secrets

log = logging.getLogger("integration.vendor")

RETRYABLE = frozenset({408, 425, 429, 500, 502, 503, 504})


class CircuitOpen(RuntimeError):
    """The vendor has been failing: calls are paused until the circuit closes."""


class Unavailable(RuntimeError):
    """The vendor didn't answer usefully after every retry."""


class Disallowed(RuntimeError):
    """A request to a host outside allowed_hosts, or not over https."""


class Circuit:
    def __init__(self, failures: int, open_s: float, clock: Callable[[], float] = time.monotonic) -> None:
        self.limit, self.open_s, self.clock = failures, open_s, clock
        self.failures, self.opened_at = 0, 0.0
        self.lock = threading.Lock()

    def is_open(self) -> bool:
        with self.lock:
            return self.failures >= self.limit and self.clock() - self.opened_at < self.open_s

    def check(self) -> None:
        if self.is_open():
            raise CircuitOpen("the vendor is failing; calls are paused")

    def success(self) -> None:
        with self.lock:
            self.failures = 0

    def failure(self) -> None:
        with self.lock:
            self.failures += 1
            if self.failures >= self.limit:
                self.opened_at = self.clock()


class Bucket:
    def __init__(
        self,
        per_s: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.rate, self.capacity, self.clock, self.sleep = per_s, max(1.0, per_s), clock, sleep
        self.tokens, self.at = self.capacity, clock()
        self.lock = threading.Lock()

    def wait(self) -> None:
        while True:
            with self.lock:
                now = self.clock()
                self.tokens = min(self.capacity, self.tokens + (now - self.at) * self.rate)
                self.at = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                pause = (1 - self.tokens) / self.rate
            self.sleep(pause)


class Vendor:
    def __init__(
        self,
        cfg: Outbound,
        secrets: Secrets,
        *,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.cfg, self.secrets, self.sleep, self.clock = cfg, secrets, sleep, clock
        self.circuit = Circuit(cfg.circuit_failures, cfg.circuit_open_s, clock)
        self.bucket = Bucket(cfg.rate_per_s, clock, sleep)
        self._token: tuple[str, float] | None = None
        self._token_lock = threading.Lock()
        cert = None
        if cfg.auth.get("type") == "mtls":
            cert = (secrets.path(cfg.auth["cert"]), secrets.path(cfg.auth["private_key"]))
        kwargs: dict[str, Any] = {"timeout": cfg.timeout_s, "follow_redirects": False, "trust_env": False}
        if transport is not None:
            kwargs["transport"] = transport
        elif cert:
            kwargs["cert"] = cert
        self.http = httpx.Client(**kwargs)

    # -------------------------------------------------------------- safety
    def url(self, path: str) -> str:
        url = urljoin(self.cfg.base_url + "/", path.lstrip("/"))
        self.allowed(url)
        return url

    def allowed(self, url: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme != "https" or (parsed.hostname or "").lower() not in self.cfg.allowed_hosts:
            raise Disallowed(f"{parsed.scheme}://{parsed.hostname}: not an allowed vendor host")

    # -------------------------------------------------------------- auth
    def _auth_headers(self, refresh: bool = False) -> dict[str, str]:
        kind = self.cfg.auth.get("type")
        if kind == "api-key":
            return {self.cfg.auth["header"]: self.secrets.current(self.cfg.auth["key"])}
        if kind == "oauth2-client-credentials":
            return {"Authorization": f"Bearer {self._bearer(refresh)}"}
        return {}

    def _bearer(self, refresh: bool) -> str:
        with self._token_lock:
            if self._token and not refresh and self.clock() < self._token[1]:
                return self._token[0]
            token_url = self.cfg.auth["token_url"]
            self.allowed(token_url)
            form = {"grant_type": "client_credentials"}
            if self.cfg.auth.get("scope"):
                form["scope"] = self.cfg.auth["scope"]
            resp = self.http.post(
                token_url,
                data=form,
                auth=(
                    self.secrets.current(self.cfg.auth["client_id"]),
                    self.secrets.current(self.cfg.auth["client_secret"]),
                ),
            )
            if resp.status_code != 200:
                raise Unavailable(f"token endpoint answered {resp.status_code}")
            body = resp.json()
            ttl = max(60.0, float(body.get("expires_in", 300)) - 60)
            self._token = (str(body["access_token"]), self.clock() + ttl)
            return self._token[0]

    # -------------------------------------------------------------- calls
    def request(
        self,
        method: str,
        path: str,
        *,
        idempotency_key: str,
        json: Any = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        url = self.url(path)
        self.circuit.check()
        refreshed = False
        for attempt in range(self.cfg.retries + 1):
            self.bucket.wait()
            hdrs = {**(headers or {}), **self._auth_headers(refreshed), "Idempotency-Key": idempotency_key}
            try:
                resp = self.http.request(method, url, json=json, params=params, headers=hdrs)
            except httpx.TransportError as exc:
                log.warning(
                    "vendor %s %s: %s (attempt %d)",
                    method,
                    urlparse(url).path,
                    type(exc).__name__,
                    attempt + 1,
                )
                retry_after = None
            else:
                if (
                    resp.status_code == 401
                    and self.cfg.auth.get("type") == "oauth2-client-credentials"
                    and not refreshed
                ):
                    refreshed = True  # the token may have been revoked early: fetch a new one once
                    continue
                if resp.status_code < 300:
                    self.circuit.success()
                    return resp
                if 300 <= resp.status_code < 400:
                    self.circuit.success()
                    raise Permanent(f"vendor redirected ({resp.status_code}); redirects aren't followed")
                if resp.status_code not in RETRYABLE:
                    self.circuit.success()  # the vendor is up; this request is wrong
                    raise Permanent(f"vendor answered {resp.status_code}")
                retry_after = resp.headers.get("Retry-After")
                log.warning(
                    "vendor %s %s: %d (attempt %d)", method, urlparse(url).path, resp.status_code, attempt + 1
                )
            if attempt == self.cfg.retries:
                break
            self.sleep(self._backoff(attempt, retry_after))
        self.circuit.failure()
        raise Unavailable(
            f"vendor {method} {urlparse(url).path} failed after {self.cfg.retries + 1} attempts"
        )

    @staticmethod
    def _backoff(attempt: int, retry_after: str | None) -> float:
        if retry_after and retry_after.isdigit():
            return min(60.0, float(retry_after))
        return random.uniform(0, min(30.0, 0.5 * 2**attempt))  # noqa: S311 - jitter, not security

    def get(self, path: str, **kw: Any) -> httpx.Response:
        return self.request("GET", path, **kw)

    def post(self, path: str, **kw: Any) -> httpx.Response:
        return self.request("POST", path, **kw)

    def put(self, path: str, **kw: Any) -> httpx.Response:
        return self.request("PUT", path, **kw)

    def close(self) -> None:
        self.http.close()
