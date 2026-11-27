"""The connector: webhooks in, vendor calls out.

Routes: /webhooks/<name> (the only ones the platform exposes outside the cluster), /healthz,
/readyz and /metrics (in-cluster only). Background threads: the outbox relay, the outbound
consumer when any @outbound handler exists, and the pull sync (pull.py) when integration.yaml
names pull sources.

Start-up refuses to run when integration.yaml is invalid, a secret it names isn't mounted, a
webhook or pull source has no handler, or a PHI/payments product's Kafka connections aren't TLS.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest

from . import cohorts, errors, events, flags, http_metrics, management, telemetry
from . import mapping as _mapping  # noqa: F401 - registers the handlers
from .audit import Auditor, make_sink, require_encryption
from .component import COMPONENT
from .config import Config, Webhook, load
from .contracts import Contracts, summary_line
from .handlers import check as check_handlers
from .secrets import SecretMissing, Secrets
from .settings import Settings
from .store import MemoryStore, PostgresStore, Store
from .webhooks import Receiver

log = logging.getLogger("integration")


@dataclass
class Runtime:
    settings: Settings
    cfg: Config
    secrets: Secrets
    store: Store
    contracts: Contracts
    auditor: Auditor
    threads: list[Any] = field(default_factory=list)
    ready: threading.Event = field(default_factory=threading.Event)


def startup_problems(cfg: Config, secrets: Secrets, contracts: Contracts) -> list[str]:
    problems = check_handlers(
        [w.name for w in cfg.webhooks],
        [c.dataset for c in contracts.by_topic.values()],
        [p.name for p in cfg.pulls],
    )
    for name in cfg.secrets:
        try:
            secrets.values(name)
        except SecretMissing as exc:
            problems.append(str(exc))
    return problems


def build(
    settings: Settings,
    *,
    store: Store | None = None,
    auditor: Auditor | None = None,
    cfg: Config | None = None,
) -> Runtime:
    cfg = cfg or load(settings.config_path)
    secrets = Secrets(settings.secrets_dir)
    contracts = Contracts.load(settings.contracts_dir)
    problems = startup_problems(cfg, secrets, contracts)
    if problems:
        raise RuntimeError("the connector can't start: " + "; ".join(problems))
    if settings.environment in ("staging", "production") and not (
        settings.database_url and settings.kafka_bootstrap
    ):
        problems.append(
            "staging and production need INTEGRATION_DATABASE_URL and INTEGRATION_KAFKA_BOOTSTRAP"
        )
        raise RuntimeError("the connector can't start: " + "; ".join(problems))
    if settings.kafka_bootstrap:
        require_encryption(settings.kafka_config, cfg.data_class, "events")
    if auditor is None:
        required = (
            settings.audit_required
            if settings.audit_required is not None
            else cfg.data_class in ("phi", "payments")
        )
        auditor = Auditor(
            make_sink(
                settings.audit_sink,
                bootstrap=settings.audit_kafka_bootstrap,
                topic=settings.audit_topic,
                config=settings.audit_kafka_config,
                data_class=cfg.data_class,
            ),
            service=COMPONENT,
            product=settings.product,
            environment=settings.environment,
            data_class=cfg.data_class,
            required=required,
        )
    if store is None:
        store = PostgresStore(settings.database_url) if settings.database_url else MemoryStore()
    return Runtime(settings, cfg, secrets, store, contracts, auditor)


def start_workers(rt: Runtime) -> None:
    """The pull sync (it writes to the outbox), then the relay and the outbound consumer
    (deployed: Kafka configured)."""
    s = rt.settings
    if rt.cfg.pulls:
        from .pull import Sync
        from .vendor import Vendor

        needs_vendor = any(p.adapter == "http-cursor" for p in rt.cfg.pulls)
        sync = Sync(
            rt.cfg,
            rt.store,
            rt.contracts,
            rt.auditor,
            source=f"{s.product}/{COMPONENT}",
            vendor=Vendor(rt.cfg.outbound, rt.secrets) if needs_vendor and rt.cfg.outbound else None,
        )
        sync.start()
        rt.threads.append(sync)
    if not s.kafka_bootstrap:
        log.warning("no Kafka configured: events stay in the outbox")
        return
    from .consumer import Outbound
    from .relay import Relay, kafka_producer
    from .vendor import Vendor

    kafka = {"bootstrap.servers": s.kafka_bootstrap, **s.kafka_config}
    relay = Relay(rt.store, rt.contracts, kafka_producer(kafka))
    relay.start()
    rt.threads.append(relay)
    if rt.cfg.outbound is not None:
        from confluent_kafka import Consumer

        consumer = Consumer(
            {
                **kafka,
                "group.id": f"{COMPONENT}.outbound",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        out = Outbound(
            rt.store,
            rt.contracts,
            Vendor(rt.cfg.outbound, rt.secrets),
            rt.auditor,
            consumer,
            lambda: kafka_producer(kafka),
            component=COMPONENT,
        )
        out.start()
        rt.threads.append(out)


def _report_datasets(stop: threading.Event) -> None:
    from . import pull

    while not stop.wait(60):
        for line in (summary_line(), pull.summary_line()):
            if line:
                log.info(line)


async def _read_capped(request: Request, limit: int) -> bytes | None:
    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > limit:
        return None
    chunks, size = [], 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


def create_app(
    settings: Settings | None = None, *, runtime: Runtime | None = None, workers: bool = True
) -> Any:
    settings = settings or Settings()
    rt = runtime or build(settings)
    receiver = Receiver(
        rt.cfg, rt.secrets, rt.store, rt.contracts, rt.auditor, source=f"{settings.product}/{COMPONENT}"
    )
    app = FastAPI(title=COMPONENT, docs_url=None, redoc_url=None, openapi_url=None)
    telemetry.setup(COMPONENT, app=app)
    errors.start_errors()
    flags.configure(settings)
    events.register(REGISTRY)

    def route(hook: Webhook) -> Any:
        async def handle(request: Request) -> Response:
            body = await _read_capped(request, hook.max_body_kb * 1024)
            if body is None:
                return JSONResponse({"error": "too large"}, status_code=413)
            answer = receiver.receive(hook, dict(request.headers), body)
            return JSONResponse(answer.body, status_code=answer.status, headers=answer.headers)

        return handle

    for hook in rt.cfg.webhooks:
        app.add_api_route(hook.path, route(hook), methods=["POST"], include_in_schema=False)

    @app.get("/healthz", include_in_schema=False)
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    def readyz() -> Response:
        dead = [t.name for t in rt.threads if not t.is_alive()]
        if dead:
            return JSONResponse({"status": "not ready", "stopped": dead}, status_code=503)
        return JSONResponse({"status": "ready"})

    # Requests, errors and latency per route, on the management port (http_metrics.py).
    app.add_middleware(http_metrics.HttpMetrics)
    app.add_middleware(cohorts.CohortMiddleware)
    app.add_middleware(errors.TraceResponseMiddleware)
    app.state.runtime = rt
    if workers:
        stop = threading.Event()
        start_workers(rt)
        threading.Thread(target=_report_datasets, args=(stop,), name="datasets", daemon=True).start()
        cohorts.start_reporting()
        events.start_reporting()
    return app


def metrics_body() -> tuple[bytes, str]:
    """What the management port serves (management.py): every metric, the product events counter
    (valxel_events_total) among them."""
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST


def main() -> None:
    import uvicorn

    settings = Settings()
    telemetry.setup_logging(COMPONENT)
    # /metrics on the management port (MANAGEMENT_PORT, 8081), apart from the API, as in the Java
    # templates: the mesh lets the data plane's metrics collector reach that port only.
    management.serve(metrics_body, host="0.0.0.0")  # noqa: S104  NOSONAR: inside the container; the collector scrapes it from another pod
    uvicorn.run(create_app(settings), host="0.0.0.0", port=settings.port, log_config=None)  # noqa: S104
