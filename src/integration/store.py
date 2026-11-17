"""The inbox, the outbox and what was sent: the connector's own tables in the product's Postgres.

inbox     every webhook delivery seen, by (webhook, delivery id): a delivery the vendor sends
          again is answered without publishing twice. Holds the body's SHA-256, never the body.
outbox    events to publish, written in the same transaction as the inbox row, so a delivery is
          either accepted with its events or not at all; a relay publishes them to Kafka.
sent      product events already sent to the vendor, by event id: a redelivered event isn't sent
          twice (the vendor also gets the event id as the Idempotency-Key).
"""

from __future__ import annotations

import json
import threading
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


class Store(Protocol):
    def seen(self, webhook: str, delivery_id: str) -> bool: ...
    def accept(self, webhook: str, delivery_id: str, body_sha256: str, events: list[Outgoing]) -> bool: ...
    def refuse(self, webhook: str, delivery_id: str, body_sha256: str, error: str) -> bool: ...
    def claim(self, limit: int = 100) -> list[Queued]: ...
    def published(self, ids: list[int]) -> None: ...
    def was_sent(self, event_id: str) -> bool: ...
    def mark_sent(self, event_id: str, dataset: str) -> None: ...
    def prune(self, days: int) -> int: ...


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
