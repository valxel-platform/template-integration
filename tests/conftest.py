from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import integration.mapping  # noqa: F401 - registers the example handlers
from integration.audit import Auditor, MemorySink
from integration.config import load
from integration.contracts import Contracts
from integration.secrets import Secrets
from integration.store import MemoryStore

ROOT = Path(__file__).resolve().parent.parent
SIGNING = "whsec-synthetic-current"


@pytest.fixture
def secrets_dir(tmp_path: Path) -> Path:
    d = tmp_path / "secrets"
    d.mkdir()
    (d / "lab-results-signing").write_text(SIGNING + "\n")
    (d / "lab-client-id").write_text("synthetic-client\n")
    (d / "lab-client-secret").write_text("synthetic-secret\n")
    return d


@pytest.fixture
def cfg() -> Any:
    return load(str(ROOT / "integration.yaml"))


@pytest.fixture
def contracts() -> Contracts:
    return Contracts.load(ROOT / "contracts")


@pytest.fixture
def secrets(secrets_dir: Path) -> Secrets:
    return Secrets(str(secrets_dir))


@pytest.fixture
def sink() -> MemorySink:
    return MemorySink()


@pytest.fixture
def auditor(sink: MemorySink) -> Auditor:
    return Auditor(sink, service="template-integration", product="emr", environment="test", data_class="phi")


@pytest.fixture
def store() -> MemoryStore:
    return MemoryStore()
