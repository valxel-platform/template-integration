"""integration.yaml: who's on the other side, how each side proves who it is, and the limits.

People approve this file at G4 with the design; the coder writes it from the HLD's Integrations
section. It holds names of secrets, never their values.

    name: template-integration
    vendor: example-labs
    data_class: phi              # the most sensitive data that crosses: phi needs baa: true
    baa: true                    # a signed business associate agreement with the vendor
    inbound:
      webhooks:
        - name: lab-results
          path: /webhooks/lab-results
          signature: { type: hmac-sha256, header: X-Signature, secret: lab-results-signing,
                       timestamp_header: X-Timestamp, tolerance_s: 300 }
          delivery_id_header: X-Delivery-Id
          max_body_kb: 512
          rate_per_minute: 600
    outbound:
      base_url: https://api.example-labs.com
      allowed_hosts: [api.example-labs.com]
      auth: { type: oauth2-client-credentials, token_url: https://auth.example-labs.com/oauth/token,
              client_id: lab-client-id, client_secret: lab-client-secret, scope: orders.write }
      timeout_s: 10
      retries: 5
      rate_per_s: 10
      circuit: { failures: 5, open_s: 60 }
    pull:                          # sources this connector polls (pull.py); each record -> @pull
      - name: test-catalog
        adapter: http-cursor       # through `outbound` above: its hosts, auth, rate and retries
        every_minutes: 60
        request: { path: /v1/tests, items: tests, cursor_param: cursor, next_cursor: next_cursor,
                   page_size_param: limit, page_size: 200, since_param: updated_since }
        deletions: tombstone       # the source marks gone records (the handler returns Delete)
        backfill_from: "2025-01-01"
      - name: protocols
        adapter: object-store      # files in a bucket: each new or changed object -> @pull
        every_minutes: 15
        store: { kind: s3, resource: lab-drop, prefix: protocols/ }   # the bucket bound to this connector
        deletions: full-listing    # an object gone since the last full listing -> Delete
        permissions_field: allow   # the dataset field holding who may see a record; empty: nobody

`store.resource` names the object-bucket the design binds to this connector (Resources, `reads:
<connector>`): its name comes from the binding's environment variable (<RESOURCE>_BUCKET, through
workload.yaml's dependencies), and the connector's identity may read it and nothing else. `bucket`
names one directly, for local runs only; the SDLC tool refuses it on a product's branch.

`deletions`: tombstone (the handler says so), full-listing (each run lists everything and compares;
object-store only) or none. `permissions_field`: when set, a record the handler gives no
principals is published visible to nobody, never to everyone.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

SIGNATURES = ("hmac-sha256",)
AUTHS = ("oauth2-client-credentials", "api-key", "mtls", "none")
DATA_CLASSES = ("general", "payments", "phi")
ADAPTERS = ("http-cursor", "object-store")
DELETIONS = ("tombstone", "full-listing", "none")
STORES = ("s3",)  # gcs and azure-blob follow the GCP and Azure local flavours (CP-120, CP-121)
_NAME = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
_SECRET = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Webhook:
    name: str
    path: str
    secret: str
    header: str = "X-Signature"
    timestamp_header: str = "X-Timestamp"
    tolerance_s: int = 300
    delivery_id_header: str = "X-Delivery-Id"
    max_body_kb: int = 512
    rate_per_minute: int = 600


@dataclass(frozen=True)
class Outbound:
    base_url: str
    allowed_hosts: tuple[str, ...]
    auth: dict[str, str]
    timeout_s: float = 10.0
    retries: int = 5
    rate_per_s: float = 10.0
    circuit_failures: int = 5
    circuit_open_s: float = 60.0


@dataclass(frozen=True)
class Pull:
    """One source this connector polls (pull.py)."""

    name: str
    adapter: str
    every_minutes: int
    deletions: str = "none"
    backfill_from: str = ""
    permissions_field: str = ""
    # http-cursor
    path: str = ""
    items: str = "items"
    cursor_param: str = "cursor"
    next_cursor: str = "next_cursor"
    page_size_param: str = ""
    page_size: int = 100
    since_param: str = ""
    params: dict[str, str] = field(default_factory=dict)
    # object-store
    store_kind: str = ""
    bucket: str = ""
    resource: str = ""
    prefix: str = ""

    def bucket_name(self, env: Mapping[str, str]) -> str:
        """The bucket to read: named, or from the bound resource's environment variable."""
        if self.bucket:
            return self.bucket
        var = resource_env(self.resource, "bucket")
        name = env.get(var, "")
        if not name:
            raise ValueError(
                f"{var} isn't set: bind {self.resource} in workload.yaml (dependencies.resources)"
            )
        return name


def resource_env(resource: str, output: str) -> str:
    """A bound resource's output as an environment variable, as the SDLC tool binds it
    (valxel_sdlc.resources.env_name): lab-drop's bucket -> LAB_DROP_BUCKET."""
    return re.sub(r"[^A-Z0-9]+", "_", f"{resource}_{output}".upper()).strip("_")


@dataclass(frozen=True)
class Config:
    name: str
    vendor: str
    data_class: str
    baa: bool
    webhooks: tuple[Webhook, ...] = ()
    outbound: Outbound | None = None
    pulls: tuple[Pull, ...] = ()
    secrets: tuple[str, ...] = field(default=())  # every secret named, for the start-up check


def parse(raw: dict[str, Any]) -> Config:
    problems: list[str] = []
    name, vendor = str(raw.get("name") or ""), str(raw.get("vendor") or "")
    if not _NAME.match(name):
        problems.append("name must be the component's name")
    if not _NAME.match(vendor):
        problems.append("vendor must name who's on the other side (lowercase, dashes)")
    data_class = str(raw.get("data_class") or "")
    if data_class not in DATA_CLASSES:
        problems.append(
            f"data_class must be one of {', '.join(DATA_CLASSES)}: the most sensitive data that crosses"
        )
    baa = bool(raw.get("baa", False))
    if data_class == "phi" and not baa:
        problems.append(
            "PHI crosses to this vendor: set baa: true once a business associate agreement is signed"
        )
    secrets: list[str] = []

    def secret(where: str, value: Any) -> str:
        v = str(value or "")
        if not _SECRET.match(v):
            problems.append(f"{where}: name a mounted secret (lowercase, dots, dashes), never its value")
        secrets.append(v)
        return v

    hooks = []
    seen: set[str] = set()
    for i, w in enumerate(((raw.get("inbound") or {}).get("webhooks")) or []):
        where = f"inbound.webhooks[{i}]"
        wname, path = str(w.get("name") or ""), str(w.get("path") or "")
        if not _NAME.match(wname) or wname in seen:
            problems.append(f"{where}: a unique name is required")
        seen.add(wname)
        if not re.match(r"^/webhooks/[a-z0-9-]+$", path):
            problems.append(
                f"{where}: path must be /webhooks/<name>: only that prefix is reachable from outside"
            )
        sig = w.get("signature") or {}
        if sig.get("type") not in SIGNATURES:
            problems.append(
                f"{where}: signature.type must be one of {', '.join(SIGNATURES)}: every webhook is signed"
            )
        tolerance = int(sig.get("tolerance_s", 300))
        if not 30 <= tolerance <= 600:
            problems.append(f"{where}: signature.tolerance_s must be 30-600 (older deliveries are replays)")
        body, rate = int(w.get("max_body_kb", 512)), int(w.get("rate_per_minute", 600))
        if not 1 <= body <= 10240:
            problems.append(f"{where}: max_body_kb must be 1-10240")
        if rate < 1:
            problems.append(f"{where}: rate_per_minute must be 1 or more")
        hooks.append(
            Webhook(
                name=wname,
                path=path,
                secret=secret(f"{where}.signature.secret", sig.get("secret")),
                header=str(sig.get("header") or "X-Signature"),
                timestamp_header=str(sig.get("timestamp_header") or "X-Timestamp"),
                tolerance_s=tolerance,
                delivery_id_header=str(w.get("delivery_id_header") or "X-Delivery-Id"),
                max_body_kb=body,
                rate_per_minute=rate,
            )
        )
    out = None
    o = raw.get("outbound")
    if o:
        base = urlparse(str(o.get("base_url") or ""))
        hosts = tuple(str(h).lower() for h in o.get("allowed_hosts") or [])
        if base.scheme != "https":
            problems.append("outbound.base_url must be https")
        if not hosts or (base.hostname or "") not in hosts:
            problems.append("outbound.allowed_hosts must list base_url's host: calls go to these hosts only")
        auth = dict(o.get("auth") or {})
        kind = auth.get("type")
        if kind not in AUTHS:
            problems.append(f"outbound.auth.type must be one of {', '.join(AUTHS)}")
        if kind == "none" and data_class != "general":
            problems.append(f"outbound.auth.type none can't carry {data_class} data")
        if kind == "oauth2-client-credentials":
            token = urlparse(str(auth.get("token_url") or ""))
            if token.scheme != "https" or (token.hostname or "") not in hosts:
                problems.append("outbound.auth.token_url must be https on an allowed host")
            secret("outbound.auth.client_id", auth.get("client_id"))
            secret("outbound.auth.client_secret", auth.get("client_secret"))
        if kind == "api-key":
            secret("outbound.auth.key", auth.get("key"))
            if not auth.get("header"):
                problems.append("outbound.auth.header names the header the key goes in")
        if kind == "mtls":
            secret("outbound.auth.cert", auth.get("cert"))
            secret("outbound.auth.private_key", auth.get("private_key"))
        retries = int(o.get("retries", 5))
        if not 0 <= retries <= 10:
            problems.append("outbound.retries must be 0-10")
        circuit = o.get("circuit") or {}
        out = Outbound(
            base_url=str(o["base_url"]).rstrip("/") if o.get("base_url") else "",
            allowed_hosts=hosts,
            auth={k: str(v) for k, v in auth.items()},
            timeout_s=float(o.get("timeout_s", 10)),
            retries=retries,
            rate_per_s=float(o.get("rate_per_s", 10)),
            circuit_failures=int(circuit.get("failures", 5)),
            circuit_open_s=float(circuit.get("open_s", 60)),
        )
    pulls = _pulls(raw.get("pull") or [], out, problems)
    if not hooks and out is None and not pulls:
        problems.append("name at least one inbound webhook, an outbound connection or a pull source")
    if problems:
        raise ConfigError("integration.yaml: " + "; ".join(problems))
    return Config(
        name=name,
        vendor=vendor,
        data_class=data_class,
        baa=baa,
        webhooks=tuple(hooks),
        outbound=out,
        pulls=tuple(pulls),
        secrets=tuple(sorted(set(secrets))),
    )


_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_FIELD = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]{0,63}$")


def _pulls(raw: Any, out: Outbound | None, problems: list[str]) -> list[Pull]:
    if not isinstance(raw, list):
        problems.append("pull must be a list of sources")
        return []
    pulls, seen = [], set()
    for i, p in enumerate(raw):
        where = f"pull[{i}]"
        if not isinstance(p, dict):
            problems.append(f"{where} must be a mapping")
            continue
        name, adapter = str(p.get("name") or ""), str(p.get("adapter") or "")
        if not _NAME.match(name) or name in seen:
            problems.append(f"{where}: a unique name is required")
        seen.add(name)
        if adapter not in ADAPTERS:
            problems.append(f"{where}: adapter must be one of {', '.join(ADAPTERS)}")
        every = int(p.get("every_minutes", 15))
        if not 1 <= every <= 1440:
            problems.append(f"{where}: every_minutes must be 1-1440")
        deletions = str(p.get("deletions") or "none")
        if deletions not in DELETIONS:
            problems.append(f"{where}: deletions must be one of {', '.join(DELETIONS)}")
        if deletions == "full-listing" and adapter != "object-store":
            problems.append(
                f"{where}: deletions full-listing needs a source that can list everything (object-store)"
            )
        backfill = str(p.get("backfill_from") or "")
        if backfill and not _DATE.match(backfill):
            problems.append(f"{where}: backfill_from is a date, YYYY-MM-DD")
        perms = str(p.get("permissions_field") or "")
        if perms and not _FIELD.match(perms):
            problems.append(f"{where}: permissions_field names a field of the dataset")
        req, store = dict(p.get("request") or {}), dict(p.get("store") or {})
        if adapter == "http-cursor":
            if out is None:
                problems.append(
                    f"{where}: an http-cursor source is read through `outbound` (hosts, auth, limits): add it"
                )
            path = str(req.get("path") or "")
            if not path.startswith("/") or "://" in path:
                problems.append(f"{where}: request.path is a path on outbound.base_url, starting with /")
            size = int(req.get("page_size", 100))
            if not 1 <= size <= 1000:
                problems.append(f"{where}: request.page_size must be 1-1000")
            for key in ("items", "cursor_param", "next_cursor", "page_size_param", "since_param"):
                if req.get(key) is not None and not _FIELD.match(str(req[key])):
                    problems.append(f"{where}: request.{key} names a field or parameter")
        if adapter == "object-store":
            if str(store.get("kind") or "") not in STORES:
                problems.append(f"{where}: store.kind must be one of {', '.join(STORES)}")
            if bool(store.get("bucket")) == bool(store.get("resource")):
                problems.append(
                    f"{where}: store.resource names the bucket bound to this connector (or, for local "
                    "runs only, store.bucket names one): exactly one of them"
                )
            elif store.get("resource") and not _NAME.match(str(store["resource"])):
                problems.append(f"{where}: store.resource is the resource's name in the design")
        pulls.append(
            Pull(
                name=name,
                adapter=adapter,
                every_minutes=every,
                deletions=deletions,
                backfill_from=backfill,
                permissions_field=perms,
                path=str(req.get("path") or ""),
                items=str(req.get("items") or "items"),
                cursor_param=str(req.get("cursor_param") or "cursor"),
                next_cursor=str(req.get("next_cursor") or "next_cursor"),
                page_size_param=str(req.get("page_size_param") or ""),
                page_size=int(req.get("page_size", 100)),
                since_param=str(req.get("since_param") or ""),
                params={str(k): str(v) for k, v in dict(req.get("params") or {}).items()},
                store_kind=str(store.get("kind") or ""),
                bucket=str(store.get("bucket") or ""),
                resource=str(store.get("resource") or ""),
                prefix=str(store.get("prefix") or ""),
            )
        )
    return pulls


def load(path: str) -> Config:
    try:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"integration.yaml isn't YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError("integration.yaml must be a mapping")
    return parse(raw)
