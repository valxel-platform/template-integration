"""Pull mode (pull.py): pages, cursors, backfill and catch-up, deletions, parking, permissions, a
source that's down, and the promise that a crash between a page and its cursor loses nothing."""

from __future__ import annotations

import copy
import json
import threading
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml

from integration import handlers
from integration.app import startup_problems
from integration.config import ConfigError, parse
from integration.contracts import Contracts
from integration.handlers import Delete, Emit, check, pull
from integration.pull import ATTENTION_AFTER, FAILING, Object, PullError, S3Objects, Sync, objects_for
from integration.vendor import Unavailable, Vendor

from .conftest import ROOT

BASE: dict[str, Any] = yaml.safe_load((ROOT / "integration.yaml").read_text())
HTTP = {
    "name": "test-catalog",
    "adapter": "http-cursor",
    "every_minutes": 60,
    "request": {
        "path": "/v1/tests",
        "items": "tests",
        "cursor_param": "cursor",
        "next_cursor": "next",
        "page_size_param": "limit",
        "page_size": 2,
        "since_param": "updated_since",
    },
    "deletions": "tombstone",
    "backfill_from": "2025-01-01",
    "permissions_field": "allow",
}
FILES = {
    "name": "protocols",
    "adapter": "object-store",
    "every_minutes": 15,
    "store": {"kind": "s3", "bucket": "drop", "prefix": "protocols/"},
    "deletions": "full-listing",
    "request": {"page_size": 2},
}


def config(*pulls: dict[str, Any], **over: Any) -> Any:
    raw = copy.deepcopy(BASE)
    raw["pull"] = [copy.deepcopy(p) for p in pulls]
    raw.update(over)
    return parse(raw)


def refused(*pulls: Any, raw_pull: Any = None, **over: Any) -> str:
    raw = copy.deepcopy(BASE)
    raw["pull"] = raw_pull if raw_pull is not None else list(pulls)
    raw.update(over)
    with pytest.raises(ConfigError) as err:
        parse(raw)
    return str(err.value)


@pytest.fixture
def pull_contracts(tmp_path: Path) -> Contracts:
    d = tmp_path / "contracts"
    d.mkdir()
    lock = yaml.safe_load((ROOT / "contracts" / "lock.yaml").read_text())
    lock["lab-test"] = {"topic": "emr.lab-test.v1", "version": "1.0.0"}
    lock["protocol"] = {"topic": "emr.protocol.v1", "version": "1.0.0"}
    (d / "lock.yaml").write_text(yaml.safe_dump(lock))
    for name in ("lab-result", "lab-order"):
        (d / f"{name}.json").write_text((ROOT / "contracts" / f"{name}.json").read_text())
    (d / "lab-test.json").write_text(
        json.dumps(
            {
                "type": "object",
                "required": ["code", "name"],
                "properties": {
                    "code": {"type": "string", "minLength": 1},
                    "name": {"type": "string", "maxLength": 20},
                    "allow": {"type": "array", "items": {"type": "string"}},
                },
            }
        )
    )
    (d / "protocol.json").write_text(
        json.dumps(
            {
                "type": "object",
                "required": ["path"],
                "properties": {"path": {"type": "string"}, "size": {"type": "integer"}},
            }
        )
    )
    return Contracts.load(d)


@pytest.fixture
def registered():
    """The test handlers, removed again so other tests' start-up checks see only the example."""

    @pull("test-catalog")
    def catalog(rec: dict[str, Any]) -> list[Emit | Delete]:
        if rec.get("boom"):
            raise KeyError("code")
        if rec.get("retired"):
            return [Delete("lab-test", key=rec["code"])]
        data = {"code": rec["code"], "name": rec["name"]}
        if "allow" in rec:
            data["allow"] = rec["allow"]
        return [Emit("lab-test", key=rec["code"], data=data)]

    @pull("protocols")
    def protocol(rec: dict[str, Any]) -> list[Emit | Delete]:
        if rec["content"] == b"bad":
            return [Emit("protocol", key=rec["key"], data={"path": rec["key"], "size": "big"})]
        return [Emit("protocol", key=rec["key"], data={"path": rec["key"], "size": len(rec["content"])})]

    yield
    handlers.PULL.pop("test-catalog", None)
    handlers.PULL.pop("protocols", None)


class API:
    """A paged vendor API: pages by cursor, recording what each call asked for."""

    def __init__(self, pages: dict[str, dict[str, Any]]) -> None:
        self.pages, self.calls = pages, []
        self.fail_on: str | None = None

    def get(self, path: str, *, params: dict[str, Any], idempotency_key: str) -> Any:
        self.calls.append(dict(params))
        cursor = str(params.get("cursor", ""))
        if self.fail_on is not None and cursor == self.fail_on:
            raise Unavailable("the vendor is down")
        body = self.pages[cursor]
        return httpx.Response(200, json=body)


def pages(*items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out = {}
    for i, page in enumerate(items):
        cursor = "" if i == 0 else f"c{i}"
        nxt = f"c{i + 1}" if i + 1 < len(items) else None
        out[cursor] = {"tests": page, "next": nxt}
    return out


def t(code: str, **kw: Any) -> dict[str, Any]:
    return {"code": code, "name": f"test {code}", **kw}


def sync(cfg, store, contracts, auditor, **kw: Any) -> Sync:
    clock = kw.pop("clock", lambda: 1_700_000_000.0)
    return Sync(cfg, store, contracts, auditor, source="emr/template-integration", clock=clock, **kw)


def outbox(store) -> list[dict[str, Any]]:
    return [dict(r) for r in store.outbox]


# configuration


def test_a_pull_source_is_parsed_with_its_request_and_store():
    cfg = config(HTTP, FILES)
    http, files = cfg.pulls
    assert (http.adapter, http.path, http.items, http.next_cursor, http.page_size) == (
        "http-cursor",
        "/v1/tests",
        "tests",
        "next",
        2,
    )
    assert (http.deletions, http.backfill_from, http.permissions_field) == (
        "tombstone",
        "2025-01-01",
        "allow",
    )
    assert (files.store_kind, files.bucket, files.prefix, files.deletions) == (
        "s3",
        "drop",
        "protocols/",
        "full-listing",
    )


def test_a_connector_may_be_pull_only():
    raw = copy.deepcopy(BASE)
    raw.pop("inbound")
    raw.pop("outbound")
    raw["pull"] = [FILES]
    assert parse(raw).pulls[0].name == "protocols"


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"adapter": "graph-sharepoint"}, "adapter must be one of"),
        ({"every_minutes": 0}, "every_minutes must be 1-1440"),
        ({"deletions": "sometimes"}, "deletions must be one of"),
        ({"deletions": "full-listing"}, "full-listing needs a source that can list everything"),
        ({"backfill_from": "last year"}, "backfill_from is a date"),
        ({"permissions_field": "who can see"}, "permissions_field names a field"),
        ({"request": {"path": "https://elsewhere.test/v1"}}, "request.path is a path on outbound.base_url"),
        ({"request": {"path": "/v1", "page_size": 5000}}, "page_size must be 1-1000"),
        ({"request": {"path": "/v1", "items": "a b"}}, "request.items names a field"),
        ({"name": "Not A Name"}, "a unique name is required"),
    ],
)
def test_a_wrong_pull_source_is_refused(change, message):
    assert message in refused({**HTTP, **change})


def test_object_store_sources_name_a_supported_store_and_a_bucket():
    assert "store.kind must be one of s3" in refused({**FILES, "store": {"kind": "ftp", "bucket": "b"}})
    assert "store.bucket names the bucket" in refused({**FILES, "store": {"kind": "s3"}})


def test_http_sources_need_outbound_and_names_are_unique_and_shape_is_checked():
    assert "read through `outbound`" in refused(HTTP, outbound=None)
    assert "a unique name is required" in refused(FILES, FILES)
    assert "pull must be a list" in refused(raw_pull="nope")
    assert "pull[0] must be a mapping" in refused("nope")


def test_start_up_needs_a_handler_for_every_pull_source_and_a_source_for_every_handler(registered):
    hooks, datasets = ["lab-results"], ["lab-order"]  # the example's webhook and outbound dataset
    assert check(hooks, datasets, ["test-catalog", "protocols", "orphan"]) == [
        "pull source orphan has no @pull handler"
    ]
    assert check(hooks, datasets, ["test-catalog"]) == [
        "@pull(protocols) names no pull source in integration.yaml"
    ]
    with pytest.raises(ValueError, match="already has a handler"):
        pull("protocols")(lambda r: [])


def test_the_connectors_start_up_check_covers_pull_sources(registered, secrets, contracts):
    problems = startup_problems(config(HTTP), secrets, contracts)
    assert problems == ["@pull(protocols) names no pull source in integration.yaml"]


# http-cursor


def test_pages_become_whole_record_events_and_the_cursor_follows_them(
    registered, store, pull_contracts, auditor, sink
):
    cfg = config(HTTP)
    api = API(pages([t("A1"), t("A2")], [t("A3")]))
    run = sync(cfg, store, pull_contracts, auditor, vendor=api).run_once(cfg.pulls[0])
    assert run is not None and (run.seen, run.published, run.deleted, run.parked) == (3, 3, 0, 0)
    rows = outbox(store)
    assert [(r["topic"], r["key"]) for r in rows] == [("emr.lab-test.v1", k) for k in ("A1", "A2", "A3")]
    ev = rows[0]["value"]
    assert ev["type"] == "lab-test" and ev["dataschema"] == "lab-test@1.0.0"
    assert ev["source"] == "emr/template-integration" and ev["data"]["code"] == "A1"
    assert rows[0]["headers"] == {"valxel-dataset": "lab-test", "valxel-pull": "test-catalog"}
    # the first run backfills from the configured date, then follows the cursor with the page size
    assert api.calls == [{"limit": 2, "updated_since": "2025-01-01"}, {"limit": 2, "cursor": "c1"}]
    # caught up: the next run asks for what changed since this run began
    assert store.pull_state("test-catalog") == {"cursor": "", "since": "2023-11-14T22:13:20Z"}
    record = store.runs[run.id - 1]
    assert record["outcome"] == "succeeded" and record["cursor_from"] == ""
    assert record["cursor_to"] == "since=2023-11-14T22:13:20Z" and record["published"] == 3
    audits = [e["data"] for e in sink.events]
    assert [a["action"] for a in audits] == ["integration.received", "integration.received"]
    assert audits[0]["resource"] == {"type": "pull-record", "ids": ["A1", "A2"]}
    assert audits[0]["details"] == {
        "pull": "test-catalog",
        "run": run.id,
        "records": 2,
        "published": 2,
        "deleted": 0,
    }
    assert "name" not in json.dumps(audits)  # ids and counts, never field values


def test_the_next_run_is_incremental(registered, store, pull_contracts, auditor):
    cfg = config(HTTP)
    api = API(pages([t("A1")]))
    s = sync(cfg, store, pull_contracts, auditor, vendor=api)
    s.run_once(cfg.pulls[0])
    s.run_once(cfg.pulls[0])
    assert api.calls[1] == {"limit": 2, "updated_since": "2023-11-14T22:13:20Z"}


def test_without_a_since_parameter_the_next_run_rereads_from_the_last_cursor(
    registered, store, pull_contracts, auditor
):
    src = copy.deepcopy(HTTP)
    src["request"].pop("since_param")
    src.pop("backfill_from")
    cfg = config(src)
    api = API(pages([t("A1")], [t("A2")]))
    s = sync(cfg, store, pull_contracts, auditor, vendor=api)
    s.run_once(cfg.pulls[0])
    assert api.calls[0] == {"limit": 2}
    assert store.pull_state("test-catalog") == {"cursor": "c1", "since": ""}
    s.run_once(cfg.pulls[0])
    assert api.calls[2] == {"limit": 2, "cursor": "c1"}


def test_a_crash_between_a_page_and_its_cursor_loses_nothing(registered, store, pull_contracts, auditor):
    cfg = config(HTTP)
    api = API(pages([t("A1"), t("A2")], [t("A3"), t("A4")], [t("A5")]))
    s = sync(cfg, store, pull_contracts, auditor, vendor=api)
    store.fail_next_page = False
    real = store.pull_page
    calls = {"n": 0}

    def crash_on_second(*a: Any, **kw: Any) -> None:
        calls["n"] += 1
        if calls["n"] == 2:
            store.fail_next_page = True  # the transaction for page 2 fails: nothing of it is kept
        real(*a, **kw)

    store.pull_page = crash_on_second  # type: ignore[method-assign]
    first = s.run_once(cfg.pulls[0])
    assert first is not None and store.runs[first.id - 1]["outcome"] == "failed"
    assert [r["key"] for r in outbox(store)] == ["A1", "A2"]
    assert store.pull_state("test-catalog")["cursor"] == "c1"  # the cursor didn't pass page 2
    store.pull_page = real  # type: ignore[method-assign]
    second = s.run_once(cfg.pulls[0])
    assert second is not None and store.runs[second.id - 1]["outcome"] == "succeeded"
    assert api.calls[-2]["cursor"] == "c1"  # page 2 read again, not skipped
    assert sorted({r["key"] for r in outbox(store)}) == ["A1", "A2", "A3", "A4", "A5"]
    assert second.cursor_from == "cursor=c1,since=2025-01-01"


def test_a_source_that_keeps_failing_needs_attention_and_catches_up_after(
    registered, store, pull_contracts, auditor
):
    cfg = config(HTTP)
    api = API(pages([t("A1")], [t("A2")]))
    api.fail_on = "c1"
    s = sync(cfg, store, pull_contracts, auditor, vendor=api)
    for _ in range(ATTENTION_AFTER):
        run = s.run_once(cfg.pulls[0])
        assert run is not None and store.runs[run.id - 1]["outcome"] == "failed"
    assert s.needs_attention() == {"test-catalog": "Unavailable: the vendor is down"}
    assert FAILING.labels("test-catalog")._value.get() == ATTENTION_AFTER
    api.fail_on = None
    s.run_once(cfg.pulls[0])
    assert s.needs_attention() == {} and FAILING.labels("test-catalog")._value.get() == 0
    assert [r["key"] for r in outbox(store)] == ["A1", "A2"]  # resumed at the saved cursor: page 1 once
    assert store.pull_last_success("test-catalog") is not None


def test_a_tombstone_becomes_a_deleted_event_with_only_the_key(registered, store, pull_contracts, auditor):
    cfg = config(HTTP)
    api = API(pages([t("A1", retired=True)]))
    run = sync(cfg, store, pull_contracts, auditor, vendor=api).run_once(cfg.pulls[0])
    assert run is not None and (run.published, run.deleted) == (0, 1)
    (row,) = outbox(store)
    assert (
        row["topic"] == "emr.lab-test.v1" and row["key"] == "A1"
    )  # same key: compaction replaces the record
    assert row["value"]["type"] == "lab-test.deleted" and row["value"]["data"] == {"key": "A1"}
    assert "dataschema" not in row["value"] and row["headers"]["valxel-deleted"] == "true"


def test_a_record_that_breaks_its_contract_or_its_handler_is_parked_and_the_run_carries_on(
    registered, store, pull_contracts, auditor
):
    cfg = config(HTTP)
    api = API(
        pages([t("A1", name="a name far too long for the contract"), {"code": "A2", "boom": True}, t("A3")])
    )
    run = sync(cfg, store, pull_contracts, auditor, vendor=api).run_once(cfg.pulls[0])
    assert run is not None and (run.seen, run.published, run.parked) == (3, 1, 2)
    assert store.runs[run.id - 1]["outcome"] == "succeeded"
    assert store.parked == [
        {"source": "test-catalog", "key": "item-0", "field": "name", "rule": "maxLength"},
        {"source": "test-catalog", "key": "item-1", "field": "", "rule": "KeyError"},
    ]
    assert "far too long" not in json.dumps(store.parked)  # never the value
    assert [r["key"] for r in outbox(store)] == ["A3"]


def test_unmappable_permissions_mean_visible_to_nobody(registered, store, pull_contracts, auditor):
    cfg = config(HTTP)
    api = API(pages([t("A1", allow=["group:lab"]), t("A2"), t("A3", allow=[]), t("A4", allow="everyone")]))
    sync(cfg, store, pull_contracts, auditor, vendor=api).run_once(cfg.pulls[0])
    allow = {r["key"]: r["value"]["data"]["allow"] for r in outbox(store)}
    assert allow == {"A1": ["group:lab"], "A2": [], "A3": [], "A4": []}


def test_a_response_of_the_wrong_shape_fails_the_run(registered, store, pull_contracts, auditor):
    cfg = config(HTTP)
    api = API({"": {"results": []}})
    s = sync(cfg, store, pull_contracts, auditor, vendor=api)
    run = s.run_once(cfg.pulls[0])
    assert run is not None
    assert store.runs[run.id - 1]["error"] == "PullError: the response has no list at 'tests'"
    assert issubclass(PullError, RuntimeError)


def test_an_http_source_reads_through_the_outbound_client(
    registered, store, pull_contracts, auditor, secrets
):
    cfg = config(HTTP)
    seen: list[httpx.Request] = []

    def answer(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/oauth/token":
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 3600})
        return httpx.Response(200, json={"tests": [t("A1")], "next": None})

    v = Vendor(cfg.outbound, secrets, transport=httpx.MockTransport(answer), sleep=lambda s: None)
    sync(cfg, store, pull_contracts, auditor, vendor=v).run_once(cfg.pulls[0])
    call = next(r for r in seen if r.url.path == "/v1/tests")
    assert call.url.host == "api.example-labs.test" and call.headers["authorization"] == "Bearer tok"
    assert dict(call.url.params) == {"limit": "2", "updated_since": "2025-01-01"}


def test_an_http_source_without_the_client_fails_its_run(registered, store, pull_contracts, auditor):
    cfg = config(HTTP)
    run = sync(cfg, store, pull_contracts, auditor).run_once(cfg.pulls[0])
    assert run is not None and "needs the outbound client" in store.runs[run.id - 1]["error"]


def test_a_dataset_without_a_contract_is_parked(registered, store, contracts, auditor):
    cfg = config(HTTP)
    run = sync(cfg, store, contracts, auditor, vendor=API(pages([t("A1"), t("A2", retired=True)]))).run_once(
        cfg.pulls[0]
    )
    assert run is not None and run.parked == 2
    assert {p["rule"] for p in store.parked} == {"no contract: add the dataset to the design"}


def test_one_run_per_source_at_a_time(registered, store, pull_contracts, auditor):
    cfg = config(HTTP)
    s = sync(cfg, store, pull_contracts, auditor, vendor=API(pages([t("A1")])))
    with store.pull_lock("test-catalog") as got:
        assert got
        assert s.run_once(cfg.pulls[0]) is None  # another replica has it
    assert s.run_once(cfg.pulls[0]) is not None


def test_a_source_without_a_handler_is_a_bug(store, pull_contracts, auditor):
    cfg = config(HTTP)
    with pytest.raises(RuntimeError, match="has no @pull handler"):
        sync(cfg, store, pull_contracts, auditor).run_once(cfg.pulls[0])


# scheduling


def test_sources_run_on_their_own_schedules(registered, store, pull_contracts, auditor):
    cfg = config(HTTP, FILES)
    now = {"t": 10_000.0}
    s = sync(cfg, store, pull_contracts, auditor, clock=lambda: now["t"])
    assert [p.name for p in s.due()] == ["test-catalog", "protocols"]
    s.health["test-catalog"].last_started = now["t"]
    s.health["protocols"].last_started = now["t"]
    now["t"] += 15 * 60
    assert [p.name for p in s.due()] == ["protocols"]
    now["t"] += 45 * 60
    assert [p.name for p in s.due()] == ["test-catalog", "protocols"]


def test_the_sync_thread_runs_due_sources_until_stopped(registered, store, pull_contracts, auditor):
    cfg = config(HTTP)
    api = API(pages([t("A1")]))
    s = sync(cfg, store, pull_contracts, auditor, vendor=api, tick_s=0.01)
    done = threading.Event()
    real = s.run_once

    def once(p: Any) -> Any:
        try:
            return real(p)
        finally:
            done.set()

    s.run_once = once  # type: ignore[method-assign]
    s.start()
    assert done.wait(5)
    s.stop.set()
    s.join(5)
    assert not s.is_alive() and [r["key"] for r in outbox(store)] == ["A1"]


# object-store


class Bucket:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files, self.reads = dict(files), []
        self.stop_after: int | None = None
        self.sync: Sync | None = None

    def list(self, bucket: str, prefix: str):
        assert (bucket, prefix) == ("drop", "protocols/")
        for i, (k, v) in enumerate(sorted(self.files.items())):
            if self.stop_after is not None and i == self.stop_after and self.sync is not None:
                self.sync.stop.set()
            yield Object(key=k, etag=f"e{hash(v) & 0xFFFF}")

    def read(self, bucket: str, key: str) -> bytes:
        self.reads.append(key)
        return self.files[key]


def test_objects_are_read_once_until_they_change_and_deleted_when_gone(
    registered, store, pull_contracts, auditor
):
    cfg = config(FILES)
    bucket = Bucket({"protocols/a.txt": b"aaa", "protocols/b.txt": b"bb", "protocols/c.txt": b"c"})
    s = sync(cfg, store, pull_contracts, auditor, objects={"s3": bucket})
    run = s.run_once(cfg.pulls[0])
    assert run is not None and (run.seen, run.published) == (3, 3)
    assert next(r["value"]["data"] for r in outbox(store)) == {"path": "protocols/a.txt", "size": 3}
    assert store.pull_state("protocols") == {"listed": 3, "last_key": "protocols/c.txt"}

    bucket.reads.clear()
    bucket.files["protocols/b.txt"] = b"changed"
    del bucket.files["protocols/c.txt"]
    run = s.run_once(cfg.pulls[0])
    assert run is not None and bucket.reads == ["protocols/b.txt"]  # a is unchanged: not read again
    assert (run.seen, run.published, run.deleted) == (1, 1, 1)
    last = outbox(store)[-1]
    assert last["key"] == "protocols/c.txt" and last["value"]["type"] == "protocol.deleted"
    assert "protocols/c.txt" not in store.pull_known("protocols")  # forgotten with its deletion

    run = s.run_once(cfg.pulls[0])
    assert run is not None and run.deleted == 0  # deleted once, not every run


def test_a_partial_listing_never_deletes(registered, store, pull_contracts, auditor):
    cfg = config(FILES)
    bucket = Bucket({"protocols/a.txt": b"a", "protocols/b.txt": b"b", "protocols/c.txt": b"c"})
    s = sync(cfg, store, pull_contracts, auditor, objects={"s3": bucket})
    s.run_once(cfg.pulls[0])
    bucket.stop_after, bucket.sync = 1, s  # the connector stops during the next listing
    run = s.run_once(cfg.pulls[0])
    assert run is not None and run.deleted == 0
    assert set(store.pull_known("protocols")) == {"protocols/a.txt", "protocols/b.txt", "protocols/c.txt"}


def test_a_broken_object_is_parked_and_not_retried_until_it_changes(
    registered, store, pull_contracts, auditor
):
    cfg = config(FILES)
    bucket = Bucket({"protocols/a.txt": b"bad"})
    s = sync(cfg, store, pull_contracts, auditor, objects={"s3": bucket})
    run = s.run_once(cfg.pulls[0])
    assert run is not None and run.parked == 1
    assert store.parked[0] == {
        "source": "protocols",
        "key": "protocols/a.txt",
        "field": "size",
        "rule": "type",
    }
    bucket.reads.clear()
    s.run_once(cfg.pulls[0])
    assert bucket.reads == []


def test_without_full_listing_a_gone_object_is_left_alone(registered, store, pull_contracts, auditor):
    cfg = config({**FILES, "deletions": "none"})
    bucket = Bucket({"protocols/a.txt": b"a"})
    s = sync(cfg, store, pull_contracts, auditor, objects={"s3": bucket})
    s.run_once(cfg.pulls[0])
    bucket.files.clear()
    run = s.run_once(cfg.pulls[0])
    assert run is not None and run.deleted == 0


def test_s3_lists_by_page_and_reads_bodies():
    class Body:
        def read(self) -> bytes:
            return b"x"

    class Client:
        def get_paginator(self, name: str) -> Any:
            assert name == "list_objects_v2"
            return self

        def paginate(self, **kw: Any) -> Any:
            assert kw == {"Bucket": "drop", "Prefix": "p/"}
            return [{"Contents": [{"Key": "p/a", "ETag": '"abc"'}]}, {}]

        def get_object(self, **kw: Any) -> Any:
            return {"Body": Body()}

    s3 = S3Objects(Client())
    assert list(s3.list("drop", "p/")) == [Object("p/a", "abc")]
    assert s3.read("drop", "p/a") == b"x"
    with pytest.raises(ValueError, match="no object store client for ftp"):
        objects_for("ftp")


def test_the_connector_starts_the_sync_with_the_outbound_client(
    registered, secrets_dir, store, auditor, monkeypatch
):
    from integration import app as appmod

    monkeypatch.setattr(Sync, "run", lambda self: None)  # started, but no real vendor calls in a test
    from integration.settings import Settings

    settings = Settings(
        config_path=str(ROOT / "integration.yaml"),
        contracts_dir=str(ROOT / "contracts"),
        secrets_dir=str(secrets_dir),
        product="emr",
        audit_sink="memory",
    )
    rt = appmod.build(settings, store=store, auditor=auditor, cfg=config(HTTP, FILES))
    appmod.start_workers(rt)  # no Kafka: pulled events wait in the outbox
    (s,) = rt.threads
    try:
        assert isinstance(s, Sync) and isinstance(s.vendor, Vendor) and s.source == "emr/template-integration"
    finally:
        s.stop.set()
        s.join(5)
