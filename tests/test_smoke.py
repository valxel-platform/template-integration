from __future__ import annotations

from xml.etree import ElementTree as ET

import httpx
import pytest

from deployed import smoke

from .conftest import ROOT


def run(tmp_path, monkeypatch, answer):
    def fake(method):
        def call(url, **kw):
            return httpx.Response(answer(method, url, kw))

        return call

    monkeypatch.setattr(httpx, "post", fake("POST"))
    monkeypatch.setattr(httpx, "get", fake("GET"))
    out = tmp_path / "smoke.xml"
    code = smoke.main(
        ["--base-url", "https://hooks.test/", "--config", str(ROOT / "integration.yaml"), "--junit", str(out)]
    )
    return code, ET.fromstring(out.read_text())  # noqa: S314 - our own file


def test_a_locked_down_connector_passes(tmp_path, monkeypatch):
    code, suite = run(tmp_path, monkeypatch, lambda m, u, kw: 401 if m == "POST" else 404)
    assert code == 0 and suite.get("tests") == "5" and suite.get("failures") == "0"


@pytest.mark.parametrize("leak", ["/metrics", "/webhooks/lab-results"])
def test_an_exposed_path_or_an_open_webhook_fails(tmp_path, monkeypatch, leak):
    code, suite = run(
        tmp_path, monkeypatch, lambda m, u, kw: 200 if u.endswith(leak) else (401 if m == "POST" else 404)
    )
    assert code == 1 and int(suite.get("failures")) >= 1
