"""Key discovery module.

- Scans public GitHub repositories (via the GitHub Search API) for strings
  that look like AI API keys.
- Also fetches recent public Gists and scans their files.
- Supports regex patterns for both API keys and OAuth tokens.
- Keeps track of already-scanned URLs in `logs/processed_urls.txt` to avoid
  duplicate work.
- Returns a list of tuples: (provider, api_key, source_url, line_num)
"""

import re
import os
from pathlib import Path

import requests as _requests
from github import Github

# ── Provider regex patterns ────────────────────────────────────────────────────
# Rules:
#   - Patterns must be specific enough to avoid broad false positives.
#   - The literal prefix (before the first regex metacharacter) is used as the
#     GitHub code-search term.  Short/ambiguous prefixes are skipped for code
#     search but still applied during gist scanning.
PATTERNS = {
    # ── Inference / LLM APIs ──────────────────────────────────────────────────
    "openai":      r"sk-[A-Za-z0-9]{48}",
    "anthropic":   r"sk-ant-[A-Za-z0-9_-]{95,}",
    "gemini":      r"AIza[0-9A-Za-z\-_]{35}",
    "groq":        r"gsk_[A-Za-z0-9]{52}",
    "xai":         r"xai-[A-Za-z0-9_\-]{64,}",
    "replicate":   r"r8_[A-Za-z0-9]{40}",
    "perplexity":  r"pplx-[a-zA-Z0-9]{40,}",
    "huggingface": r"hf_[A-Za-z0-9]{34}",
    "openrouter":  r"or_[A-Za-z0-9]{40}",
    "kimi":        r"sk-[A-Za-z0-9]{40}",          # Moonshot/Kimi keys
    # ── Cloud / infra ─────────────────────────────────────────────────────────
    "amazon_bedrock": r"AKIA[0-9A-Z]{16}",
    # ── OAuth tokens ─────────────────────────────────────────────────────────
    "google_oauth": r"ya29\.[A-Za-z0-9_\-]+",
    "github_oauth": r"ghp_[A-Za-z0-9]{36}",
    # Azure AD JWTs: exactly three base64url parts (hdr.payload.sig).
    # Old pattern was too broad — matched Docker signing blobs and Minecraft tokens.
    "azure_oauth":  r"eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}",
}

# Providers whose literal prefix is too short / generic to use in GitHub code
# search (would return millions of noise results).  They are still matched
# during gist content scanning.
_SKIP_CODE_SEARCH = {"azure_oauth", "google_oauth"}

# Literal prefixes that are too broad for code search regardless of length.
# e.g. "sk-" → millions of Stripe, OpenAI, and other results combined.
# More specific sub-patterns (sk-ant- for Anthropic) still get searched.
_SKIP_PREFIXES = {"sk-"}

# GitHub code search caps at 1 000 results (10 pages × 100).  Going beyond
# that returns 403.
_CODE_SEARCH_MAX = 1000

# ── Log / state files ─────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parents[1] / "logs"
PROCESSED_FILE = LOG_DIR / "processed_urls.txt"


def _load_processed() -> set:
    if not PROCESSED_FILE.is_file():
        return set()
    with open(PROCESSED_FILE, "r") as f:
        return {line.strip() for line in f if line.strip() and not line.startswith("#")}


def _append_processed(url: str):
    with open(PROCESSED_FILE, "a") as f:
        f.write(url + "\n")


def _literal_prefix(pattern: str) -> str:
    """Return the longest literal text prefix before the first regex metachar.

    Handles common escaped sequences: \\. becomes a literal dot in the prefix.
    """
    meta = re.compile(r"(?<!\\)[\\^$.|?*+()\[\]{}]")
    m = meta.search(pattern)
    raw = pattern[: m.start()] if m else pattern
    # Collapse escaped dots to real dots for the search term
    return raw.replace("\\.", ".")


def _extract_from_content(content: str, url: str, discoveries: list):
    """Scan content against all PATTERNS; append (provider, key, url, line) tuples."""
    for provider, pattern in PATTERNS.items():
        for match in re.finditer(pattern, content):
            key = match.group(0)
            line_num = content[: match.start()].count("\n") + 1
            discoveries.append((provider, key, url, line_num))


def _search_github_code(query: str, gh: Github) -> list:
    """Return up to _CODE_SEARCH_MAX code-search results, respecting the 1 000-result cap."""
    results = []
    try:
        code_results = gh.search_code(query, sort="indexed", order="desc")
        for i, file in enumerate(code_results):
            if i >= _CODE_SEARCH_MAX:
                break
            results.append(file)
    except Exception as e:
        print(f"GitHub code search error for '{query}': {e}")
    return results


def _gist_auth_headers(github_token: str | None) -> dict:
    if github_token:
        return {"Authorization": f"token {github_token}"}
    return {}


def discover_keys(github_token: str | None = None, max_pages: int = 2) -> list:
    """Return a list of (provider, key, source_url, line_num) tuples."""
    gh = Github(github_token) if github_token else Github()
    auth_headers = _gist_auth_headers(github_token)
    processed = _load_processed()
    discoveries = []

    # ── 1. GitHub code search ─────────────────────────────────────────────────
    # Search each provider's literal prefix across all file types (no language
    # filter — keys get leaked in .env, .js, .yaml, .txt, etc.)
    seen_prefixes: set[str] = set()
    for provider, pattern in PATTERNS.items():
        if provider in _SKIP_CODE_SEARCH:
            continue
        prefix = _literal_prefix(pattern)
        if not prefix or len(prefix) < 3 or prefix in _SKIP_PREFIXES:
            # Too short or too generic — would flood results with noise
            continue
        if prefix in seen_prefixes:
            # Multiple providers share the same prefix (e.g. openai + kimi → sk-)
            # Already queued; the content scan will classify to the right provider.
            continue
        seen_prefixes.add(prefix)

        query = f'"{prefix}" in:file'
        results = _search_github_code(query, gh)
        for file in results:
            raw_url = file.html_url
            if raw_url in processed:
                continue
            try:
                content = file.decoded_content.decode(errors="ignore")
            except Exception:
                continue
            _extract_from_content(content, raw_url, discoveries)
            _append_processed(raw_url)

    # ── 2. Public Gists ───────────────────────────────────────────────────────
    # Authenticated requests → 5 000 req/hr instead of 60.
    try:
        resp = _requests.get(
            "https://api.github.com/gists/public",
            params={"per_page": 100},
            headers=auth_headers,
            timeout=15,
        )
        if resp.status_code == 200:
            for gist in resp.json():
                gist_url = gist.get("html_url")
                if not gist_url or gist_url in processed:
                    continue
                for fdata in gist.get("files", {}).values():
                    raw = fdata.get("raw_url")
                    if not raw:
                        continue
                    try:
                        file_resp = _requests.get(
                            raw, headers=auth_headers, timeout=10
                        )
                        if file_resp.status_code != 200:
                            continue
                        _extract_from_content(file_resp.text, gist_url, discoveries)
                    except Exception:
                        continue
                _append_processed(gist_url)
        else:
            print(
                f"GitHub public gist fetch error: {resp.status_code} – {resp.text[:120]}"
            )
    except Exception as e:
        print(f"Error fetching public gists: {e}")

    return discoveries
