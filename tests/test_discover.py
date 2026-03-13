"""Unit tests for discover.py — no live GitHub calls."""
import re
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.discover import PATTERNS, _SKIP_PREFIXES


class TestPatterns:
    def test_all_patterns_compile(self):
        for name, pattern in PATTERNS.items():
            re.compile(pattern)  # should not raise

    def test_openai_pattern(self):
        key = "sk-" + "A" * 48
        assert re.search(PATTERNS["openai"], key)

    def test_anthropic_pattern(self):
        key = "sk-ant-" + "A" * 95
        assert re.search(PATTERNS["anthropic"], key)

    def test_openrouter_primary_pattern(self):
        key = "sk-or-v1-" + "a" * 64
        assert re.search(PATTERNS["openrouter"], key)

    def test_openrouter_pub_pattern(self):
        key = "or_" + "B" * 40
        pattern = PATTERNS.get("openrouter_pub")
        if pattern:
            assert re.search(pattern, key)

    def test_sk_not_in_skip_prefixes(self):
        """sk-or-v1- is specific enough, should NOT be skipped."""
        assert "sk-or-v1-" not in _SKIP_PREFIXES

    def test_openrouter_not_skipped(self):
        """openrouter primary pattern must be searchable."""
        PATTERNS["openrouter"][:8]
        assert "sk-or" not in _SKIP_PREFIXES
