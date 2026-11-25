"""The inbox, the outbox and what was sent: the connector's own tables in the product's Postgres.

inbox     every webhook delivery seen, by (webhook, delivery id): a delivery the vendor sends
          again is answered without publishing twice. Holds the body's SHA-256, never the body.
outbox    events to publish, written in the same transaction as the inbox row, so a delivery is
          either accepted with its events or not at all; a relay publishes them to Kafka.
sent      product events already sent to the vendor, by event id: a redelivered event isn't sent
          twice (the vendor also gets the event id as the Idempotency-Key).

For pull sources (pull.py):
pull_state   each source's cursor (or watermark), saved in the same transaction as the events read
             up to it: the cursor never runs ahead of what's in the outbox, so a crash re-reads a
             page and never skips one.
pull_seen    each object a full-listing source has read: its etag, the events it produced and the
             run that last listed it. An unchanged object isn't read again; one no longer listed
             has its events published as deleted, and is forgotten in that same transaction.
pull_runs    every run: when, how it ended, records seen, published, deleted and parked, and the
             cursor it started and ended at (the completeness record the tool reconciles).
pull_parked  records that broke their contract: source, key, field and rule, never the value.
"""

from __future__ import annotations

import contextlib
import json
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol


@dataclass(frozen=True)
class Outgoing:
    topic: str
    key: str
    value: dict[str, Any]
    headers: dict[str, str]


@dataclass(frozen=True)
class Queued:
    id: int
    topic: str
    key: str
    value: dict[str, Any]
    headers: dict[str, str]


@dataclass
class Run:
    source: str
    id: int = 0
    cursor_from: str = ""
    cursor_to: str = ""
    seen: int = 0
    published: int = 0
    deleted: int = 0
    parked: int = 0


class Store(Protocol):
    def seen(self, webhook: str, delivery_id: str) -> bool: ...
    def accept(self, webhook: str, delivery_id: str, body_sha256: str, events: list[Outgoing]) -> bool: ...
    def refuse(self, webhook: str, delivery_id: str, body_sha256: str, error: str) -> bool: ...
    def claim(self, limit: int = 100) -> list[Queued]: ...
    def published(self, ids: list[int]) -> None: ...
    def was_sent(self, event_id: str) -> bool: ...
    def mark_sent(self, event_id: str, dataset: str) -> None: ...
    def prune(self, days: int) -> int: ...
    def pull_lock(self, source: str) -> contextlib.AbstractContextManager[bool]: ...
    def pull_state(self, source: str) -> dict[str, Any]: ...
    def pull_page(
        self,
        source: str,
        state: dict[str, Any],
        events: list[Outgoing],
        run: int,
        seen: dict[str, dict[str, Any]] | None = None,
        forget: list[str] | None = None,
    ) -> None: ...
    def pull_known(self, source: str) -> dict[str, dict[str, Any]]: ...
    def pull_unseen(self, source: str, run: int) -> dict[str, dict[str, Any]]: ...
    def pull_park(self, source: str, key: str, field: str, rule: str) -> None: ...
    def pull_run_start(self, source: str, cursor_from: str) -> int: ...
    def pull_run_end(self, run: Run, outcome: str, error: str = "") -> None: ...
    def pull_last_success(self, source: str) -> datetime | None: ...


def now() -> datetime:
    return datetime.now(UTC)


class MemoryStore:
    def __init__(self) -> None:
        self.inbox: dict[tuple[str, str], dict[str, Any]] = {}
        self.outbox: list[dict[str, Any]] = []
        self.sent: dict[str, datetime] = {}
        self._lock = threading.Lock()

    def seen(self, webhook: str, delivery_id: str) -> bool:
        return (webhook, delivery_id) in self.inbox

    def accept(self, webhook: str, delivery_id: str, body_sha256: str, events: list[Outgoing]) -> bool:
        with self._lock:
            if (webhook, delivery_id) in self.inbox:
                return False
            self.inbox[(webhook, delivery_id)] = {
                "sha256": body_sha256,
                "status": "accepted",
                "at": now(),
                "error": "",
            }
            for e in events:
                self.outbox.append(
                    {
                        "id": len(self.outbox) + 1,
                        "topic": e.topic,
                        "key": e.key,
                        "value": e.value,
                        "headers": e.headers,
                        "sent_at": None,
                        "at": now(),
                    }
                )
            return True

    def refuse(self, webhook: str, delivery_id: str, body_sha256: str, error: str) -> bool:
        with self._lock:
            if (webhook, delivery_id) in self.inbox:
                return False
            self.inbox[(webhook, delivery_id)] = {
                "sha256": body_sha256,
                "status": "failed",
                "at": now(),
                "error": error[:500],
            }
            return True

    def claim(self, limit: int = 100) -> list[Queued]:
        with self._lock:
            return [
                Queued(r["id"], r["topic"], r["key"], r["value"], r["headers"])
                for r in self.outbox
                if r["sent_at"] is None
            ][:limit]

    def published(self, ids: list[int]) -> None:
        with self._lock:
            for r in self.outbox:
                if r["id"] in ids:
                    r["sent_at"] = now()

    def was_sent(self, event_id: str) -> bool:
        return event_id in self.sent

    def mark_sent(self, event_id: str, dataset: str) -> None:
        self.sent[event_id] = now()

    def prune(self, days: int) -> int:
        before = now() - timedelta(days=days)
        with self._lock:
            n = len(self.inbox) + len(self.outbox) + len(self.sent)
            self.inbox = {k: v for k, v in self.inbox.items() if v["at"] >= before}
            self.outbox = [r for r in self.outbox if r["sent_at"] is None or r["sent_at"] >= before]
            self.sent = {k: v for k, v in self.sent.items() if v >= before}
            return n - len(self.inbox) - len(self.outbox) - len(self.sent)

    # pull sources
    def _mem_pull(self) -> None:
        if not hasattr(self, "pulls"):
            self.pulls: dict[str, dict[str, Any]] = {}
            self.seen_keys: dict[str, dict[str, tuple[int, dict[str, Any]]]] = {}
            self.runs: list[dict[str, Any]] = []
            self.parked: list[dict[str, str]] = []
            self.locks: dict[str, threading.Lock] = {}

    @contextlib.contextmanager
    def pull_lock(self, source: str) -> Iterator[bool]:
        self._mem_pull()
        lock = self.locks.setdefault(source, threading.Lock())
        got = lock.acquire(blocking=False)
        try:
            yield got
        finally:
            if got:
                lock.release()

    def pull_state(self, source: str) -> dict[str, Any]:
        self._mem_pull()
        return dict(self.pulls.get(source) or {})

    def pull_page(
        self,
        source: str,
        state: dict[str, Any],
        events: list[Outgoing],
        run: int,
        seen: dict[str, dict[str, Any]] | None = None,
        forget: list[str] | None = None,
    ) -> None:
        self._mem_pull()
        with self._lock:
            if getattr(self, "fail_next_page", False):
                self.fail_next_page = False
                raise RuntimeError("store unavailable")
            for e in events:
                self.outbox.append(
                    {
                        "id": len(self.outbox) + 1,
                        "topic": e.topic,
                        "key": e.key,
                        "value": e.value,
                        "headers": e.headers,
                        "sent_at": None,
                        "at": now(),
                    }
                )
            self.pulls[source] = dict(state)
            keys = self.seen_keys.setdefault(source, {})
            for k, info in (seen or {}).items():
                keys[k] = (run, dict(info))
            for k in forget or []:
                keys.pop(k, None)

    def pull_known(self, source: str) -> dict[str, dict[str, Any]]:
        self._mem_pull()
        return {k: dict(info) for k, (_r, info) in self.seen_keys.get(source, {}).items()}

    def pull_unseen(self, source: str, run: int) -> dict[str, dict[str, Any]]:
        self._mem_pull()
        return {k: dict(info) for k, (r, info) in sorted(self.seen_keys.get(source, {}).items()) if r != run}

    def pull_park(self, source: str, key: str, field: str, rule: str) -> None:
        self._mem_pull()
        self.parked.append({"source": source, "key": key, "field": field, "rule": rule})

    def pull_run_start(self, source: str, cursor_from: str) -> int:
        self._mem_pull()
        self.runs.append(
            {"source": source, "started_at": now(), "outcome": "running", "cursor_from": cursor_from}
        )
        return len(self.runs)

    def pull_run_end(self, run: Run, outcome: str, error: str = "") -> None:
        self._mem_pull()
        self.runs[run.id - 1].update(
            outcome=outcome,
            finished_at=now(),
            error=error[:500],
            cursor_to=run.cursor_to,
            seen=run.seen,
            published=run.published,
            deleted=run.deleted,
            parked=run.parked,
        )

    def pull_last_success(self, source: str) -> datetime | None:
        self._mem_pull()
        done = [r["finished_at"] for r in self.runs if r["source"] == source and r["outcome"] == "succeeded"]
        return max(done) if done else None


SCHEMA = """
create table if not exists integration_inbox (
  webhook text not null,
  delivery_id text not null,
  received_at timestamptz not null default now(),
  body_sha256 text not null,
  status text not null check (status in ('accepted', 'failed')),
  error text not null default '',
  primary key (webhook, delivery_id)
);
create table if not exists integration_outbox (
  id bigserial primary key,
  topic text not null,
  key text not null,
  value jsonb not null,
  headers jsonb not null default '{}',
  created_at timestamptz not null default now(),
  sent_at timestamptz
);
create index if not exists integration_outbox_unsent on integration_outbox (id) where sent_at is null;
create table if not exists integration_sent (
  event_id text primary key,
  dataset text not null,
  sent_at timestamptz not null default now()
);
create table if not exists integration_pull_state (
  source text primary key,
  state jsonb not null,
  updated_at timestamptz not null default now()
);
create table if not exists integration_pull_seen (
  source text not null,
  key text not null,
  run bigint not null,
  info jsonb not null,
  primary key (source, key)
);
create table if not exists integration_pull_runs (
  id bigserial primary key,
  source text not null,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  outcome text not null default 'running',
  cursor_from text not null default '',
  cursor_to text not null default '',
  seen integer not null default 0,
  published integer not null default 0,
  deleted integer not null default 0,
  parked integer not null default 0,
  error text not null default ''
);
create index if not exists integration_pull_runs_source on integration_pull_runs (source, id desc);
create table if not exists integration_pull_parked (
  id bigserial primary key,
  source text not null,
  key text not null,
  field text not null,
  rule text not null,
  parked_at timestamptz not null default now()
);
"""


class PostgresStore:
    def __init__(self, dsn: str) -> None:
        from psycopg_pool import ConnectionPool

        self.pool = ConnectionPool(dsn, min_size=1, max_size=5, open=True, kwargs={"autocommit": True})
        with self.pool.connection() as conn:
            conn.execute(SCHEMA)

    def seen(self, webhook: str, delivery_id: str) -> bool:
        with self.pool.connection() as conn:
            return (
                conn.execute(
                    "select 1 from integration_inbox where webhook = %s and delivery_id = %s",
                    (webhook, delivery_id),
                ).fetchone()
                is not None
            )

    def accept(self, webhook: str, delivery_id: str, body_sha256: str, events: list[Outgoing]) -> bool:
        with self.pool.connection() as conn, conn.transaction():
            row = conn.execute(
                "insert into integration_inbox (webhook, delivery_id, body_sha256, status)"
                " values (%s, %s, %s, 'accepted') on conflict do nothing returning 1",
                (webhook, delivery_id, body_sha256),
            ).fetchone()
            if row is None:
                return False
            for e in events:
                conn.execute(
                    "insert into integration_outbox (topic, key, value, headers) values (%s, %s, %s, %s)",
                    (e.topic, e.key, json.dumps(e.value), json.dumps(e.headers)),
                )
            return True

    def refuse(self, webhook: str, delivery_id: str, body_sha256: str, error: str) -> bool:
        with self.pool.connection() as conn:
            row = conn.execute(
                "insert into integration_inbox (webhook, delivery_id, body_sha256, status, error)"
                " values (%s, %s, %s, 'failed', %s) on conflict do nothing returning 1",
                (webhook, delivery_id, body_sha256, error[:500]),
            ).fetchone()
            return row is not None

    def claim(self, limit: int = 100) -> list[Queued]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                "select id, topic, key, value, headers from integration_outbox where sent_at is null"
                " order by id limit %s",
                (limit,),
            ).fetchall()
        return [Queued(int(r[0]), r[1], r[2], r[3], r[4]) for r in rows]

    def published(self, ids: list[int]) -> None:
        if ids:
            with self.pool.connection() as conn:
                conn.execute("update integration_outbox set sent_at = now() where id = any(%s)", (ids,))

    def was_sent(self, event_id: str) -> bool:
        with self.pool.connection() as conn:
            return (
                conn.execute("select 1 from integration_sent where event_id = %s", (event_id,)).fetchone()
                is not None
            )

    def mark_sent(self, event_id: str, dataset: str) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "insert into integration_sent (event_id, dataset) values (%s, %s) on conflict do nothing",
                (event_id, dataset),
            )

    def prune(self, days: int) -> int:
        n = 0
        with self.pool.connection() as conn:
            for sql in (
                "delete from integration_inbox where received_at < now() - make_interval(days => %s)",
                "delete from integration_outbox where sent_at < now() - make_interval(days => %s)",
                "delete from integration_sent where sent_at < now() - make_interval(days => %s)",
            ):
                n += int(conn.execute(sql, (days,)).rowcount or 0)
        return n

    @contextlib.contextmanager
    def pull_lock(self, source: str) -> Iterator[bool]:
        """One run per source across replicas: a session advisory lock, held for the run."""
        with self.pool.connection() as conn:
            row = conn.execute("select pg_try_advisory_lock(hashtext(%s))", (f"pull:{source}",)).fetchone()
            got = bool(row and row[0])
            try:
                yield got
            finally:
                if got:
                    conn.execute("select pg_advisory_unlock(hashtext(%s))", (f"pull:{source}",))

    def pull_state(self, source: str) -> dict[str, Any]:
        with self.pool.connection() as conn:
            row = conn.execute(
                "select state from integration_pull_state where source = %s", (source,)
            ).fetchone()
        return dict(row[0]) if row else {}

    def pull_page(
        self,
        source: str,
        state: dict[str, Any],
        events: list[Outgoing],
        run: int,
        seen: dict[str, dict[str, Any]] | None = None,
        forget: list[str] | None = None,
    ) -> None:
        with self.pool.connection() as conn, conn.transaction():
            for e in events:
                conn.execute(
                    "insert into integration_outbox (topic, key, value, headers) values (%s, %s, %s, %s)",
                    (e.topic, e.key, json.dumps(e.value), json.dumps(e.headers)),
                )
            conn.execute(
                "insert into integration_pull_state (source, state) values (%s, %s)"
                " on conflict (source) do update set state = excluded.state, updated_at = now()",
                (source, json.dumps(state)),
            )
            for k, info in (seen or {}).items():
                conn.execute(
                    "insert into integration_pull_seen (source, key, run, info) values (%s, %s, %s, %s)"
                    " on conflict (source, key) do update set run = excluded.run, info = excluded.info",
                    (source, k, run, json.dumps(info)),
                )
            if forget:
                conn.execute(
                    "delete from integration_pull_seen where source = %s and key = any(%s)", (source, forget)
                )

    def pull_known(self, source: str) -> dict[str, dict[str, Any]]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                "select key, info from integration_pull_seen where source = %s", (source,)
            ).fetchall()
        return {str(r[0]): dict(r[1]) for r in rows}

    def pull_unseen(self, source: str, run: int) -> dict[str, dict[str, Any]]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                "select key, info from integration_pull_seen where source = %s and run <> %s order by key",
                (source, run),
            ).fetchall()
        return {str(r[0]): dict(r[1]) for r in rows}

    def pull_park(self, source: str, key: str, field: str, rule: str) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "insert into integration_pull_parked (source, key, field, rule) values (%s, %s, %s, %s)",
                (source, key[:200], field[:200], rule[:100]),
            )

    def pull_run_start(self, source: str, cursor_from: str) -> int:
        with self.pool.connection() as conn:
            row = conn.execute(
                "insert into integration_pull_runs (source, cursor_from) values (%s, %s) returning id",
                (source, cursor_from[:500]),
            ).fetchone()
        if row is None:
            raise RuntimeError("the run record wasn't written")
        return int(row[0])

    def pull_run_end(self, run: Run, outcome: str, error: str = "") -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "update integration_pull_runs set finished_at = now(), outcome = %s, cursor_to = %s,"
                " seen = %s, published = %s, deleted = %s, parked = %s, error = %s where id = %s",
                (
                    outcome,
                    run.cursor_to[:500],
                    run.seen,
                    run.published,
                    run.deleted,
                    run.parked,
                    error[:500],
                    run.id,
                ),
            )

    def pull_last_success(self, source: str) -> datetime | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                "select max(finished_at) from integration_pull_runs"
                " where source = %s and outcome = 'succeeded'",
                (source,),
            ).fetchone()
        return row[0] if row and row[0] else None
