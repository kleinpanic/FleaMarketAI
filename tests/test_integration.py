"""Integration-style checks for discover module wiring (no live network)."""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure repo root is on path so `src` package resolves consistently
_REPO_ROOT = str(Path(__file__).resolve().parents[1])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from src import discover  # noqa: E402


def test_discover_has_discover_keys():
    """Main.py calls discover.discover_keys() - ensure it exists."""
    assert hasattr(discover, "discover_keys"), (
        "discover module must have discover_keys function"
    )
    assert callable(discover.discover_keys), "discover.discover_keys must be callable"


def test_discover_keys_returns_list(monkeypatch):
    """discover_keys should return a list (may be empty)."""
    # Keep this deterministic and offline.
    monkeypatch.setattr(discover, "_search_github_code", lambda *a, **k: [])

    class _Resp:
        status_code = 200
        text = "[]"
        headers = {}

        @staticmethod
        def json():
            return []

    monkeypatch.setattr(discover._requests, "get", lambda *a, **k: _Resp())

    result = discover.discover_keys(github_token=None)
    assert isinstance(result, list), (
        f"discover_keys should return list, got {type(result)}"
    )
