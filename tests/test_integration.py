"""Test that main.py correctly integrates with discover module."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import discover


def test_discover_has_discover_keys():
    """Main.py calls discover.discover_keys() - ensure it exists."""
    assert hasattr(discover, 'discover_keys'), \
        "discover module must have discover_keys function"
    assert callable(discover.discover_keys), \
        "discover.discover_keys must be callable"


def test_discover_keys_returns_list():
    """discover_keys should return a list (may be empty)."""
    # This might hit rate limits in CI, so we just check the signature works
    try:
        result = discover.discover_keys()
        assert isinstance(result, list), \
            f"discover_keys should return list, got {type(result)}"
    except Exception as e:
        # GitHub API errors are OK for this test - we just need the function to exist
        if "authentication" in str(e).lower() or "rate limit" in str(e).lower():
            pass  # Expected in CI without proper tokens
        else:
            raise
