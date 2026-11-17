"""The Postgres store, against a real database (VALXEL_TEST_DATABASE_URL); skipped without one."""

from __future__ import annotations

import os

import pytest

from integration.store import Outgoing, PostgresStore

URL = os.environ.get("VALXEL_TEST_DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not URL, reason="needs VALXEL_TEST_DATABASE_URL")


@pytest.fixture
def pg():
    store = PostgresStore(URL)
    with store.pool.connection() as conn:
        conn.execute("truncate integration_inbox, integration_outbox, integration_sent")
    yield store
    store.pool.close()


def test_inbox_outbox_and_sent(pg):
    ev = Outgoing(
        "emr.lab-result.v1",
        "O-1",
        {"id": "e1", "data": {"order_id": "O-1"}},
        {"valxel-dataset": "lab-result"},
    )
    assert pg.accept("lab-results", "d-1", "h", [ev, ev])
    assert not pg.accept("lab-results", "d-1", "h", [ev])  # a duplicate adds nothing
    assert pg.seen("lab-results", "d-1") and not pg.seen("lab-results", "d-2")
    batch = pg.claim()
    assert [q.key for q in batch] == ["O-1", "O-1"] and batch[0].value["data"] == {"order_id": "O-1"}
    assert batch[0].headers == {"valxel-dataset": "lab-result"}
    pg.published([q.id for q in batch])
    assert pg.claim() == []
    assert pg.refuse("lab-results", "d-2", "h", "breaks") and not pg.refuse("lab-results", "d-2", "h", "x")
    assert not pg.was_sent("evt-1")
    pg.mark_sent("evt-1", "lab-order")
    pg.mark_sent("evt-1", "lab-order")
    assert pg.was_sent("evt-1")
    assert pg.prune(30) == 0
    with pg.pool.connection() as conn:
        conn.execute("update integration_inbox set received_at = now() - interval '40 days'")
        conn.execute("update integration_outbox set sent_at = now() - interval '40 days'")
    assert pg.prune(30) == 4


def test_memory_store_prunes_too(store):
    store.accept("w", "d", "h", [Outgoing("t", "k", {}, {})])
    store.mark_sent("e", "d")
    assert store.prune(0) == 2  # the unsent outbox row stays
