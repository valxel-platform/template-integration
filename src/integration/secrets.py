"""Secrets from files the platform mounts (one file per secret, under INTEGRATION_SECRETS_DIR).

A file holds the current value on its first line and, while a rotation is under way, the
previous value on its second: a webhook signed with either is accepted, and outgoing calls use
the current one. Files are read again when they change, so a rotation needs no restart. Values
are never logged, put in errors or returned by any endpoint.
"""

from __future__ import annotations

import os
import re
import threading
from pathlib import Path

_NAME = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")


class SecretMissing(RuntimeError):
    pass


class Secrets:
    def __init__(self, root: str) -> None:
        self.root = Path(root)
        self._cache: dict[str, tuple[float, list[str]]] = {}
        self._lock = threading.Lock()

    def _path(self, name: str) -> Path:
        if not _NAME.match(name or ""):
            raise SecretMissing(f"secret names are lowercase words, dots and dashes: {name!r}")
        return self.root / name

    def values(self, name: str) -> list[str]:
        """Current first, then the previous one during a rotation."""
        path = self._path(name)
        try:
            mtime = os.stat(path).st_mtime
        except FileNotFoundError as exc:
            raise SecretMissing(f"secret {name} isn't mounted") from exc
        with self._lock:
            hit = self._cache.get(name)
            if hit and hit[0] == mtime:
                return hit[1]
            values = [v.strip() for v in path.read_text(encoding="utf-8").splitlines() if v.strip()][:2]
            if not values:
                raise SecretMissing(f"secret {name} is empty")
            self._cache[name] = (mtime, values)
            return values

    def current(self, name: str) -> str:
        return self.values(name)[0]

    def path(self, name: str) -> str:
        """For a client certificate or key file, used where it lies."""
        p = self._path(name)
        if not p.is_file():
            raise SecretMissing(f"secret {name} isn't mounted")
        return str(p)
