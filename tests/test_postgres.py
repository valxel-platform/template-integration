"""The Postgres store, against a real database (VALXEL_TEST_DATABASE_URL); skipped without one."""

from __future__ import annotations

import os

import pytest

from integration.store import Outgoing, PostgresStore, Run

URL = os.environ.get("VALXEL_TEST_DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not URL, reason="needs VALXEL_TEST_DATABASE_URL")


@pytest.fixture
def pg():
    store = PostgresStore(URL)
    with store.pool.connection() as conn:
        conn.execute(
            "truncate integration_inbox, integration_outbox, integration_sent, integration_pull_state,"
            " integration_pull_seen, integration_pull_runs, integration_pull_parked"
        )
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


def test_a_pull_page_is_one_transaction_with_its_cursor(pg):
    ev = Outgoing(
        "emr.lab-test.v1", "A1", {"id": "e1", "data": {"code": "A1"}}, {"valxel-dataset": "lab-test"}
    )
    assert pg.pull_state("catalog") == {}
    run = pg.pull_run_start("catalog", "")
    pg.pull_page(
        "catalog", {"cursor": "c1"}, [ev], run, seen={"o/a": {"etag": "1", "emits": [["lab-test", "A1"]]}}
    )
    assert pg.pull_state("catalog") == {"cursor": "c1"}
    batch = pg.claim()
    assert [q.key for q in batch] == ["A1"]
    pg.published([q.id for q in batch])
    with pytest.raises(Exception):  # noqa: B017 - any failure inside the page rolls the whole page back
        pg.pull_page("catalog", {"cursor": "c2"}, [Outgoing("t", "A2", {}, {"h": object()})], run)  # type: ignore[dict-item]
    assert pg.pull_state("catalog") == {"cursor": "c1"} and pg.claim() == []
    assert pg.pull_known("catalog") == {"o/a": {"etag": "1", "emits": [["lab-test", "A1"]]}}
    later = pg.pull_run_start("catalog", "cursor=c1")
    assert pg.pull_unseen("catalog", later) == {"o/a": {"etag": "1", "emits": [["lab-test", "A1"]]}}
    pg.pull_page("catalog", {"cursor": "c1"}, [], later, forget=["o/a"])
    assert pg.pull_known("catalog") == {}


def test_pull_runs_locks_and_parked_records(pg):
    assert pg.pull_last_success("catalog") is None
    rid = pg.pull_run_start("catalog", "")
    pg.pull_run_end(Run("catalog", id=rid, cursor_to="cursor=c9", seen=3, published=2, parked=1), "succeeded")
    assert pg.pull_last_success("catalog") is not None
    pg.pull_park("catalog", "A9", "name", "maxLength")
    with pg.pool.connection() as conn:
        assert conn.execute(
            "select outcome, seen, published, parked, cursor_to from integration_pull_runs"
        ).fetchone() == (
            "succeeded",
            3,
            2,
            1,
            "cursor=c9",
        )
        assert conn.execute("select key, field, rule from integration_pull_parked").fetchone() == (
            "A9",
            "name",
            "maxLength",
        )
    with pg.pull_lock("catalog") as got:
        assert got
        with pg.pull_lock("catalog") as again:
            assert not again  # another session (replica) can't run the same source
    with pg.pull_lock("catalog") as got:
        assert got
