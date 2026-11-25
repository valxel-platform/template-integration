"""Pull mode: read the sources named under `pull:` in integration.yaml on a schedule, turn each
record into product events through its @pull handler, and publish them through the outbox.

Adapters (the platform team approves a new one; the SDLC tool refuses any other name):

http-cursor   a paged HTTP API, read through the `outbound` client, so the same allowed hosts,
              auth, rate limit, retries and circuit breaker apply. Pages follow the response's
              next-cursor field. When a page has no next cursor the source is caught up: with
              `since_param` the next run asks for records changed since this run began; without
              it the next run re-reads from the last cursor.
object-store  files in a bucket (S3 for now). Each run lists the prefix; an object whose etag is
              unchanged isn't read again. With `deletions: full-listing`, an object no longer
              listed has the events it produced published as deleted.

What makes it safe to stop at any moment:

- Each page's events, the cursor after it and the objects it read are saved in ONE transaction
  (Store.pull_page). A crash before that saves nothing and the page is read again; a crash after
  it loses nothing. Events are whole records keyed by the record's key on a compacted topic, so
  reading a page twice is harmless.
- One run per source at a time, across replicas (Store.pull_lock).
- A record that breaks its contract, or that its handler can't map, is parked (source, key, field
  and rule, never the value) and the run carries on.
- With `permissions_field`, a record whose handler gives no usable principals is published with
  an empty list: visible to nobody, never to everyone.
- A source that fails `ATTENTION_AFTER` runs in a row is logged as needing attention and shown
  on the integration_pull_failing gauge; the next run resumes from the saved cursor (catch-up).

Every run leaves a completeness record (Store.pull_run_end: records seen, published, deleted and
parked, the cursor it started and ended at), and is counted in a summary line the worker logs once
a minute, counts only:

    valxel.pulls {"test-catalog":{"failed":0,"parked":1,"published":40,"runs":2,"seen":41,"succeeded":2,...}}

In staging the SDLC tool sums these per source (valxel_sdlc.reconcile): a parked record, or a
source whose every run failed, holds the release. What the runs published is also in the
valxel.datasets line, which the tool reconciles with each consumer. Each page is audited as
integration.received with the record keys and counts only.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol

from prometheus_client import Gauge

from .audit import Actor, Auditor
from .config import Config, Pull
from .contracts import ContractError, Contracts, count
from .handlers import PULL, Delete, Emit
from .store import Outgoing, Run, Store

log = logging.getLogger("integration.pull")

ATTENTION_AFTER = 3
FAILING = Gauge(
    "integration_pull_failing",
    "Runs in a row a pull source has failed (0: healthy)",
    ["source"],
)


SUMMARY = "valxel.pulls"
_runs: dict[str, dict[str, int]] = {}
_runs_lock = threading.Lock()


def _tally(run: Run, outcome: str) -> None:
    with _runs_lock:
        row = _runs.setdefault(run.source, {})
        for key, n in (
            ("runs", 1),
            (outcome, 1),
            ("seen", run.seen),
            ("published", run.published),
            ("deleted", run.deleted),
            ("parked", run.parked),
        ):
            row[key] = row.get(key, 0) + n


def summary_line() -> str | None:
    """valxel.pulls {"<source>":{"runs":n,"succeeded":n,"failed":n,"seen":n,...}} for the runs that
    ended since the last line, or None."""
    with _runs_lock:
        taken = {src: dict(sorted(row.items())) for src, row in sorted(_runs.items()) if row}
        _runs.clear()
    return f"{SUMMARY} {json.dumps(taken, separators=(',', ':'))}" if taken else None


class PullError(RuntimeError):
    """The source answered in a shape the configuration doesn't describe."""


@dataclass(frozen=True)
class Object:
    key: str
    etag: str


class Objects(Protocol):
    """An object store as pull mode reads it."""

    def list(self, bucket: str, prefix: str) -> Iterable[Object]: ...
    def read(self, bucket: str, key: str) -> bytes: ...


class S3Objects:
    """S3 through boto3 and the pod's workload identity (no keys in the connector)."""

    def __init__(self, client: Any = None) -> None:
        if client is None:
            import boto3  # type: ignore[import-not-found,unused-ignore]

            client = boto3.client("s3")
        self.client = client

    def list(self, bucket: str, prefix: str) -> Iterable[Object]:
        pages = self.client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for o in page.get("Contents") or []:
                yield Object(key=str(o["Key"]), etag=str(o.get("ETag", "")).strip('"'))

    def read(self, bucket: str, key: str) -> bytes:
        return bytes(self.client.get_object(Bucket=bucket, Key=key)["Body"].read())


def objects_for(kind: str) -> Objects:
    if kind == "s3":
        return S3Objects()
    raise ValueError(f"no object store client for {kind}")


@dataclass
class Health:
    failures: int = 0
    last_started: float = 0.0
    last_error: str = ""


@dataclass(eq=False)  # a thread must stay hashable
class Sync(threading.Thread):
    """Runs every pull source on its schedule, one run per source at a time."""

    cfg: Config
    store: Store
    contracts: Contracts
    auditor: Auditor
    source: str
    vendor: Any = None
    objects: dict[str, Objects] = field(default_factory=dict)
    clock: Callable[[], float] = time.time
    tick_s: float = 30.0

    def __post_init__(self) -> None:
        threading.Thread.__init__(self, name="pull", daemon=True)
        self.stop = threading.Event()
        self.health: dict[str, Health] = {p.name: Health() for p in self.cfg.pulls}
        self.actor = Actor(id=self.cfg.vendor, type="service")

    # scheduling
    def run(self) -> None:
        while not self.stop.is_set():
            for p in self.due():
                if self.stop.is_set():
                    break
                self.run_once(p)
            self.stop.wait(self.tick_s)

    def due(self) -> list[Pull]:
        now = self.clock()
        return [p for p in self.cfg.pulls if now - self.health[p.name].last_started >= p.every_minutes * 60]

    def run_once(self, p: Pull) -> Run | None:
        """One run of one source; None when another replica holds it."""
        handler = PULL.get(p.name)
        if handler is None:
            raise RuntimeError(f"pull source {p.name} has no @pull handler")
        with self.store.pull_lock(p.name) as got:
            if not got:
                return None
            health = self.health.setdefault(p.name, Health())
            health.last_started = self.clock()
            state = self.store.pull_state(p.name)
            run = Run(source=p.name, cursor_from=_describe(state))
            run.cursor_to = run.cursor_from
            run.id = self.store.pull_run_start(p.name, run.cursor_from)
            outcome, error = "succeeded", ""
            try:
                if p.adapter == "http-cursor":
                    self._http(p, handler, state, run)
                else:
                    self._objects(p, handler, run)
            except Exception as exc:  # noqa: BLE001 - the run ends; the next resumes from the saved cursor
                outcome, error = "failed", f"{type(exc).__name__}: {exc}"[:500]
            self.store.pull_run_end(run, outcome, error)
            _tally(run, outcome)
            self._health(p, health, outcome, error)
            log.info(
                "pull %s run %s %s: seen %d, published %d, deleted %d, parked %d",
                p.name,
                run.id,
                outcome,
                run.seen,
                run.published,
                run.deleted,
                run.parked,
            )
            return run

    def _health(self, p: Pull, health: Health, outcome: str, error: str) -> None:
        if outcome == "succeeded":
            health.failures, health.last_error = 0, ""
        else:
            health.failures += 1
            health.last_error = error
            level = logging.ERROR if health.failures >= ATTENTION_AFTER else logging.WARNING
            log.log(
                level,
                "pull %s failed %d time(s) in a row%s: %s",
                p.name,
                health.failures,
                " - needs attention" if health.failures >= ATTENTION_AFTER else "",
                error,
            )
        FAILING.labels(p.name).set(health.failures)

    def needs_attention(self) -> dict[str, str]:
        return {n: h.last_error for n, h in self.health.items() if h.failures >= ATTENTION_AFTER}

    # adapters
    def _http(self, p: Pull, handler: Any, state: dict[str, Any], run: Run) -> None:
        if self.vendor is None:
            raise RuntimeError("an http-cursor source needs the outbound client")
        began = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.clock()))
        cursor = state.get("cursor") or ""
        since = state.get("since") or p.backfill_from
        page = 0
        while not self.stop.is_set():
            page += 1
            params: dict[str, Any] = dict(p.params)
            if p.page_size_param:
                params[p.page_size_param] = p.page_size
            if cursor:
                params[p.cursor_param] = cursor
            elif since and p.since_param:
                params[p.since_param] = since
            body = self.vendor.get(
                p.path, params=params, idempotency_key=f"pull-{p.name}-{run.id}-{page}"
            ).json()
            items = body.get(p.items) if isinstance(body, dict) else None
            if not isinstance(items, list):
                raise PullError(f"the response has no list at {p.items!r}")
            nxt = body.get(p.next_cursor) or ""
            events, keys = self._map(p, handler, items, run)
            if nxt:
                new = {"cursor": nxt, "since": since}
            elif p.since_param:
                new = {"cursor": "", "since": began}
            else:
                new = {"cursor": cursor, "since": since}
            self.store.pull_page(p.name, new, events, run.id)
            self._count(events)
            self._audit(p, run, keys, len(items), events)
            run.cursor_to = _describe(new)
            if not nxt:
                return
            cursor = nxt

    def _objects(self, p: Pull, handler: Any, run: Run) -> None:
        client = self.objects.get(p.store_kind) or objects_for(p.store_kind)
        self.objects[p.store_kind] = client
        known = self.store.pull_known(p.name)
        batch: list[Object] = []
        listed = 0

        def flush() -> None:
            seen: dict[str, dict[str, Any]] = {}
            records: list[dict[str, Any]] = []
            events: list[Outgoing] = []
            keys: list[str] = []
            for o in batch:
                info = known.get(o.key)
                if info is not None and info.get("etag") == o.etag:
                    seen[o.key] = info  # unchanged: still listed, not read again
                    continue
                records.append({"key": o.key, "etag": o.etag, "content": client.read(p.bucket, o.key)})
            for rec in records:
                outs, k = self._map(p, handler, [rec], run)
                events += outs
                keys += k
                seen[rec["key"]] = {
                    "etag": rec["etag"],
                    "emits": [[e.headers["valxel-dataset"], e.key] for e in outs if not _is_delete(e)],
                }
            state = {"listed": listed, "last_key": batch[-1].key if batch else ""}
            self.store.pull_page(p.name, state, events, run.id, seen=seen)
            self._count(events)
            if records:
                self._audit(p, run, keys, len(records), events)
            run.cursor_to = _describe(state)
            batch.clear()

        for o in client.list(p.bucket, p.prefix):
            if self.stop.is_set():
                return  # a partial listing never deletes anything
            batch.append(o)
            listed += 1
            if len(batch) >= p.page_size:
                flush()
        flush()
        if p.deletions != "full-listing":
            return
        gone = self.store.pull_unseen(p.name, run.id)
        if not gone:
            return
        events = [
            self._deleted(Delete(dataset, key))
            for info in gone.values()
            for dataset, key in info.get("emits") or []
        ]
        run.deleted += len(events)
        self.store.pull_page(p.name, {"listed": listed, "last_key": ""}, events, run.id, forget=list(gone))
        self._count(events)
        self._audit(p, run, list(gone), 0, events)

    # records -> events
    def _map(self, p: Pull, handler: Any, items: list[Any], run: Run) -> tuple[list[Outgoing], list[str]]:
        events: list[Outgoing] = []
        keys: list[str] = []
        for i, rec in enumerate(items):
            run.seen += 1
            where = str(rec.get("key", f"item-{i}")) if isinstance(rec, dict) else f"item-{i}"
            try:
                if not isinstance(rec, dict):
                    raise PullError("a record must be an object")
                outs = handler(rec)
                mine = [self._deleted(o) if isinstance(o, Delete) else self._event(p, o) for o in outs]
            except ContractError as exc:
                self._park(p, run, where, exc.dataset, exc.field, exc.rule)
                continue
            except Exception as exc:  # noqa: BLE001 - a handler's bug parks the record, not the run
                self._park(p, run, where, "", "", type(exc).__name__)
                continue
            for o, e in zip(outs, mine, strict=True):
                keys.append(o.key)
                if isinstance(o, Delete):
                    run.deleted += 1
                else:
                    run.published += 1
                events.append(e)
        return events, keys

    def _park(self, p: Pull, run: Run, key: str, dataset: str, fld: str, rule: str) -> None:
        run.parked += 1
        self.store.pull_park(p.name, key, fld, rule)
        if dataset:
            count(dataset, "parked")
        log.warning("pull %s parked %s: %s (%s)", p.name, key, fld or "(record)", rule)

    def _event(self, p: Pull, e: Emit) -> Outgoing:
        contract = self.contracts.for_dataset(e.dataset)
        if contract is None:
            raise ContractError(e.dataset, "", "no contract: add the dataset to the design")
        data = dict(e.data)
        if p.permissions_field:
            allowed = data.get(p.permissions_field)
            if not (isinstance(allowed, list) and allowed and all(isinstance(a, str) and a for a in allowed)):
                data[p.permissions_field] = []  # visible to nobody, never to everyone
        checked = contract.check_whole(data)
        return Outgoing(
            topic=contract.topic,
            key=str(e.key),
            value=self._cloud_event(e.dataset, f"{e.dataset}@{contract.version}", checked),
            headers={"valxel-dataset": e.dataset, "valxel-pull": p.name},
        )

    def _deleted(self, d: Delete) -> Outgoing:
        contract = self.contracts.for_dataset(d.dataset)
        if contract is None:
            raise ContractError(d.dataset, "", "no contract: add the dataset to the design")
        return Outgoing(
            topic=contract.topic,
            key=str(d.key),
            value=self._cloud_event(f"{d.dataset}.deleted", "", {"key": str(d.key)}),
            headers={"valxel-dataset": d.dataset, "valxel-deleted": "true"},
        )

    def _cloud_event(self, type_: str, schema: str, data: dict[str, Any]) -> dict[str, Any]:
        value: dict[str, Any] = {
            "specversion": "1.0",
            "id": str(uuid.uuid4()),
            "source": self.source,
            "type": type_,
            "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(self.clock())),
            "datacontenttype": "application/json",
            "data": data,
        }
        if schema:
            value["dataschema"] = schema
        return value

    def _count(self, events: list[Outgoing]) -> None:
        for e in events:
            count(e.headers["valxel-dataset"], "deleted" if _is_delete(e) else "published")

    def _audit(self, p: Pull, run: Run, keys: list[str], records: int, events: list[Outgoing]) -> None:
        self.auditor.record(
            "integration.received",
            actor=self.actor,
            resource=("pull-record", keys),
            details={
                "pull": p.name,
                "run": run.id,
                "records": records,
                "published": sum(1 for e in events if not _is_delete(e)),
                "deleted": sum(1 for e in events if _is_delete(e)),
            },
        )


def _is_delete(e: Outgoing) -> bool:
    return e.headers.get("valxel-deleted") == "true"


def _describe(state: dict[str, Any]) -> str:
    """The cursor as the completeness record shows it."""
    if not state:
        return ""
    return ",".join(f"{k}={v}" for k, v in sorted(state.items()) if v not in ("", None))
