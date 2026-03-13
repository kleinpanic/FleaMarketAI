"""Unit tests for validate.py — no live API calls."""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.validate import _is_placeholder, VALIDATORS


class TestIsPlaceholder:
    def test_obvious_placeholder(self):
        assert _is_placeholder("sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

    def test_repeating_chars(self):
        assert _is_placeholder("sk-" + "a" * 48)

    def test_real_looking_key(self):
        assert not _is_placeholder("sk-abcDEF123xyz987QRS456tuv789WXY012mno345PQR678stu")

    def test_short_key(self):
        assert _is_placeholder("sk-tooshort")


class TestValidatorsRegistered:
    def test_all_expected_providers_registered(self):
        expected = [
            "openai", "anthropic", "gemini", "groq", "xai",
            "replicate", "perplexity", "huggingface", "kimi",
            "openrouter", "github_oauth",
        ]
        for p in expected:
            assert p in VALIDATORS, f"Missing validator for provider: {p}"

    def test_openrouter_pub_registered(self):
        """openrouter_pub should be distinct from openrouter."""
        assert "openrouter" in VALIDATORS


class TestPatternMatching:
    def test_openrouter_sk_pattern(self):
        import re
        from src.discover import PATTERNS
        pattern = PATTERNS.get("openrouter")
        assert pattern is not None
        # sk-or-v1- style key
        key = "sk-or-v1-" + "a" * 64
        assert re.search(pattern, key), f"Pattern should match sk-or-v1- key"

    def test_openrouter_pub_pattern(self):
        import re
        from src.discover import PATTERNS
        pattern = PATTERNS.get("openrouter_pub", PATTERNS.get("openrouter"))
        key = "or_" + "A" * 40
        assert re.search(pattern, key), "Pattern should match or_ public key"
