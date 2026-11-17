from __future__ import annotations

import copy
from typing import Any

import pytest
import yaml

from integration.config import ConfigError, load, parse

from .conftest import ROOT

BASE: dict[str, Any] = yaml.safe_load((ROOT / "integration.yaml").read_text())


def broken(**changes: Any) -> str:
    raw = copy.deepcopy(BASE)
    for path, value in changes.items():
        node = raw
        *parents, last = path.split("__")
        for p in parents:
            node = node[int(p)] if p.isdigit() else node[p]
        if value is None:
            node.pop(last, None)
        else:
            node[last] = value
    with pytest.raises(ConfigError) as err:
        parse(raw)
    return str(err.value)


def test_the_example_is_valid_and_names_every_secret(cfg):
    assert cfg.vendor == "example-labs" and cfg.data_class == "phi" and cfg.baa
    assert [w.path for w in cfg.webhooks] == ["/webhooks/lab-results"]
    assert cfg.secrets == ("lab-client-id", "lab-client-secret", "lab-results-signing")
    assert cfg.outbound and cfg.outbound.allowed_hosts == ("api.example-labs.test", "auth.example-labs.test")


def test_phi_needs_a_business_associate_agreement():
    assert "baa: true" in broken(baa=False)


def test_webhooks_live_under_webhooks_and_are_signed():
    assert "/webhooks/<name>" in broken(inbound__webhooks__0__path="/admin")
    assert "every webhook is signed" in broken(inbound__webhooks__0__signature__type="none")
    assert "tolerance_s" in broken(inbound__webhooks__0__signature__tolerance_s=3600)
    assert "never its value" in broken(inbound__webhooks__0__signature__secret="Bearer abc/def")


def test_outbound_is_https_to_allowed_hosts_only():
    assert "https" in broken(outbound__base_url="http://api.example-labs.test")
    assert "allowed_hosts" in broken(outbound__allowed_hosts=["other.test"])
    assert "token_url" in broken(outbound__auth__token_url="https://evil.test/token")
    assert "none can't carry phi" in broken(outbound__auth={"type": "none"})


def test_api_key_and_mtls_need_their_secrets():
    assert "header" in broken(outbound__auth={"type": "api-key", "key": "lab-key"})
    assert "outbound.auth.cert" in broken(outbound__auth={"type": "mtls", "private_key": "k"})


def test_something_must_cross(tmp_path):
    raw = {"name": "x-conn", "vendor": "v", "data_class": "general"}
    with pytest.raises(ConfigError, match="at least one"):
        parse(raw)
    bad = tmp_path / "bad.yaml"
    bad.write_text("- a list")
    with pytest.raises(ConfigError, match="mapping"):
        load(str(bad))
    bad.write_text("a: [")
    with pytest.raises(ConfigError, match="isn't YAML"):
        load(str(bad))
