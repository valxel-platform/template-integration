from __future__ import annotations

import os

import pytest

from integration.secrets import SecretMissing, Secrets


def test_current_and_previous_values_and_rotation_without_restart(secrets_dir):
    s = Secrets(str(secrets_dir))
    assert s.values("lab-results-signing") == ["whsec-synthetic-current"]
    path = secrets_dir / "lab-results-signing"
    path.write_text("whsec-new\nwhsec-synthetic-current\nignored\n")
    os.utime(path, (1, 1))  # a new mtime, as a remount gives
    assert s.values("lab-results-signing") == ["whsec-new", "whsec-synthetic-current"]
    assert s.current("lab-results-signing") == "whsec-new"


def test_missing_empty_and_odd_names_are_refused(secrets_dir):
    s = Secrets(str(secrets_dir))
    with pytest.raises(SecretMissing, match="isn't mounted"):
        s.values("nope")
    (secrets_dir / "empty").write_text("\n")
    with pytest.raises(SecretMissing, match="empty"):
        s.values("empty")
    with pytest.raises(SecretMissing, match="names"):
        s.values("../etc/passwd")
    assert s.path("lab-client-id").endswith("lab-client-id")
    with pytest.raises(SecretMissing):
        s.path("nope")
