"""Validation helpers for FleaMarket-AI.

Each public function receives the raw key/token string and returns:
    (is_valid: bool, message: str)

Design rules:
  - NEVER mark a key valid on a format check alone.
  - Every validator must call a real API endpoint and check the HTTP response.
  - Providers whose secrets cannot be verified without a second credential
    (e.g., AWS SigV4) get a format + live-account-probe combo, clearly noted.
"""

import base64
import json as _json
import re
import requests
import yaml
from pathlib import Path

CONFIG = yaml.safe_load(open(Path(__file__).resolve().parents[1] / "config.yaml"))
PROVIDERS = CONFIG.get("providers", {})

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "FleaMarket-AI/1.0"})

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_auth_header(name, prefix, key):
    return {name: f"{prefix}{key}"} if prefix else {name: key}


def _decode_jwt_payload(token: str) -> dict:
    """Base64url-decode the middle part of a JWT. Returns {} on any error."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {}
        padded = parts[1] + "=" * (-len(parts[1]) % 4)
        return _json.loads(base64.urlsafe_b64decode(padded))
    except Exception:
        return {}


def _is_placeholder(key: str) -> bool:
    """Reject obvious demo/test keys before hitting any API."""
    low = key.lower()
    if any(w in low for w in ["test", "example", "placeholder", "dummy", "sample", "your_key"]):
        return True
    # Repeating-character suffix (e.g. sk-aaaaaaa...)
    body = re.sub(r"^[a-z0-9_-]+-", "", low)
    if body and len(set(body)) == 1:
        return True
    return False


# ---------------------------------------------------------------------------
# OpenAI
# ---------------------------------------------------------------------------

def validate_openai(key):
    cfg = PROVIDERS["openai"]
    headers = _make_auth_header(cfg["header_name"], cfg.get("header_prefix", ""), key)
    try:
        r = _SESSION.get(cfg["test_endpoint"], headers=headers, timeout=10)
        if r.status_code == 200:
            return True, "OpenAI key works"
        return False, f"OpenAI returned {r.status_code}: {r.text[:80]}"
    except Exception as e:
        return False, f"OpenAI error: {e}"


# ---------------------------------------------------------------------------
# Anthropic
# ---------------------------------------------------------------------------

def validate_anthropic(key):
    cfg = PROVIDERS["anthropic"]
    headers = {
        cfg["header_name"]: key,
        "anthropic-version": cfg.get("api_version", "2023-06-01"),
        "content-type": "application/json",
    }
    payload = {
        "model": "claude-3-haiku-20240307",
        "max_tokens": 1,
        "messages": [{"role": "user", "content": "hi"}],
    }
    try:
        r = _SESSION.post(cfg["test_endpoint"], headers=headers, json=payload, timeout=10)
        if r.status_code == 200:
            return True, "Anthropic key works"
        return False, f"Anthropic returned {r.status_code}: {r.text[:80]}"
    except Exception as e:
        return False, f"Anthropic error: {e}"


# ---------------------------------------------------------------------------
# Gemini
# ---------------------------------------------------------------------------

def validate_gemini(key):
    cfg = PROVIDERS["gemini"]
    headers = {cfg["header_name"]: key, "content-type": "application/json"}
    payload = {"contents": [{"parts": [{"text": "hi"}]}]}
    try:
        r = _SESSION.post(cfg["test_endpoint"], headers=headers, json=payload, timeout=10)
        if r.status_code == 200:
            return True, "Gemini key works"
        return False, f"Gemini returned {r.status_code}: {r.text[:80]}"
    except Exception as e:
        return False, f"Gemini error: {e}"


# ---------------------------------------------------------------------------
# Groq
# ---------------------------------------------------------------------------

def validate_groq(key):
    """List models — cheap, no tokens consumed."""
    try:
        r = _SESSION.get(
            "https://api.groq.com/openai/v1/models",
            headers={"Authorization": f"Bearer {key}"},
            timeout=10,
        )
        if r.status_code == 200:
            return True, "Groq key works"
        return False, f"Groq returned {r.status_code}: {r.text[:80]}"
    except Exception as e:
        return False, f"Groq error: {e}"


# ---------------------------------------------------------------------------
# xAI (Grok)
# ---------------------------------------------------------------------------

def validate_xai(key):
    """List models via xAI's OpenAI-compatible endpoint."""
    try:
        r = _SESSION.get(
            "https://api.x.ai/v1/models",
            headers={"Authorization": f"Bearer {key}"},
            timeout=10,
        )
        if r.status_code == 200:
            return True, "xAI key works"
        return False, f"xAI returned {r.status_code}: {r.text[:80]}"
    except Exception as e:
        return False, f"xAI error: {e}"


# ---------------------------------------------------------------------------
# Replicate
# ---------------------------------------------------------------------------

def validate_replicate(key):
    """List models — lightweight, no inference cost."""
    try:
        r = _SESSION.get(
            "https://api.replicate.com/v1/models",
            headers={"Authorization": f"Token {key}"},
            timeout=10,
        )
        if r.status_code == 200:
            return True, "Replicate key works"
        return False, f"Replicate returned {r.status_code}: {r.text[:80]}"
    except Exception as e:
        return False, f"Replicate error: {e}"


# ---------------------------------------------------------------------------
# Perplexity
# ---------------------------------------------------------------------------

def validate_perplexity(key):
    """Minimal chat completion — cheapest model, 1 token max."""
    try:
        r = _SESSION.post(
            "https://api.perplexity.ai/chat/completions",
            headers={
                "Authorization": f"Bearer {key}",
                "content-type": "application/json",
            },
            json={
                "model": "sonar",
                "messages": [{"role": "user", "content": "hi"}],
                "max_tokens": 1,
            },
            timeout=15,
        )
        if r.status_code == 200:
            return True, "Perplexity key works"
        return False, f"Perplexity returned {r.status_code}: {r.text[:80]}"
    except Exception as e:
        return False, f"Perplexity error: {e}"


# ---------------------------------------------------------------------------
# Hugging Face
# ---------------------------------------------------------------------------

def validate_huggingface(key):
    """Whoami endpoint — free, confirms token is live."""
    try:
        r = _SESSION.get(
            "https://huggingface.co/api/whoami",
            headers={"Authorization": f"Bearer {key}"},
            timeout=10,
        )
        if r.status_code == 200:
            name = r.json().get("name", "unknown")
            return True, f"HuggingFace token valid (user: {name})"
        return False, f"HuggingFace returned {r.status_code}: {r.text[:80]}"
    except Exception as e:
        return False, f"HuggingFace error: {e}"


# ---------------------------------------------------------------------------
# Kimi (Moonshot AI)
# ---------------------------------------------------------------------------

def validate_kimi(key):
    try:
        r = _SESSION.get(
            "https://api.moonshot.cn/v1/models",
            headers={"Authorization": f"Bearer {key}"},
            timeout=10,
        )
        if r.status_code == 200:
            return True, "Kimi key works"
        return False, f"Kimi returned {r.status_code}: {r.text[:80]}"
    except Exception as e:
        return False, f"Kimi error: {e}"


# ---------------------------------------------------------------------------
# OpenRouter
# ---------------------------------------------------------------------------

def validate_openrouter(key):
    cfg = PROVIDERS["openrouter"]
    headers = _make_auth_header(cfg["header_name"], cfg.get("header_prefix", ""), key)
    try:
        r = _SESSION.get(cfg["test_endpoint"], headers=headers, timeout=10)
        if r.status_code == 200:
            return True, "OpenRouter key works"
        return False, f"OpenRouter returned {r.status_code}: {r.text[:80]}"
    except Exception as e:
        return False, f"OpenRouter error: {e}"


# ---------------------------------------------------------------------------
# Amazon Bedrock (AWS access key ID)
# ---------------------------------------------------------------------------

def validate_amazon_bedrock(key):
    """AWS AKIA keys need the matching secret for SigV4 — we can't fully validate.
    Instead we probe STS for the error code:
      InvalidClientTokenId  → key ID doesn't exist (discard)
      InvalidSignatureException / MissingAuthenticationToken → key ID IS real
    We never return is_valid=True; we flag real key IDs for manual review.
    """
    if not re.fullmatch(r"AKIA[0-9A-Z]{16}", key):
        return False, "AWS key format invalid (expected AKIA + 16 uppercase alnum)"
    try:
        r = _SESSION.post(
            "https://sts.amazonaws.com/",
            data={"Action": "GetCallerIdentity", "Version": "2011-06-15"},
            headers={"Authorization": f"AWS4-HMAC-SHA256 Credential={key}/bogus"},
            timeout=10,
        )
        body = r.text
        if "InvalidClientTokenId" in body:
            return False, "AWS key ID does not exist"
        if "InvalidSignatureException" in body or "MissingAuthenticationToken" in body:
            return False, "AWS key ID is REAL – secret unavailable, manual review needed"
        return False, f"AWS STS returned {r.status_code}: {body[:80]}"
    except Exception as e:
        return False, f"AWS STS error: {e}"


# ---------------------------------------------------------------------------
# Azure AD OAuth Bearer JWT
# ---------------------------------------------------------------------------

def validate_azure_oauth(key):
    """Stage 1: verify JWT issuer is Azure AD.
    Stage 2: call Azure Management API — 200 or 403 both confirm live token.
    Rejects Minecraft, Xbox, game-server, and every other non-Azure JWT.
    """
    payload = _decode_jwt_payload(key)
    if not payload:
        return False, "Could not decode JWT payload"

    iss = payload.get("iss", "")
    azure_prefixes = (
        "https://sts.windows.net/",
        "https://login.microsoftonline.com/",
    )
    if not any(iss.startswith(p) for p in azure_prefixes):
        return False, f"Not an Azure AD token (iss={iss!r})"

    try:
        r = _SESSION.get(
            "https://management.azure.com/subscriptions?api-version=2020-01-01",
            headers={"Authorization": f"Bearer {key}"},
            timeout=10,
        )
        if r.status_code == 200:
            return True, "Azure token valid – management API access confirmed"
        if r.status_code == 403:
            return True, "Azure token live (403 Forbidden – valid auth, no subscription access)"
        if r.status_code == 401:
            return False, "Azure token expired or revoked (401)"
        return False, f"Azure Management API returned {r.status_code}: {r.text[:80]}"
    except Exception as e:
        return False, f"Azure OAuth error: {e}"


# ---------------------------------------------------------------------------
# Google OAuth2 access token (ya29.*)
# ---------------------------------------------------------------------------

def validate_google_oauth(key):
    try:
        r = _SESSION.get(
            "https://www.googleapis.com/oauth2/v3/tokeninfo",
            params={"access_token": key},
            timeout=10,
        )
        if r.status_code == 200:
            scope = r.json().get("scope", "")[:120]
            return True, f"Google OAuth token valid (scope: {scope})"
        return False, f"Google tokeninfo returned {r.status_code}: {r.text[:80]}"
    except Exception as e:
        return False, f"Google OAuth error: {e}"


# ---------------------------------------------------------------------------
# GitHub personal access token (ghp_*)
# ---------------------------------------------------------------------------

def validate_github_oauth(key):
    try:
        r = _SESSION.get(
            "https://api.github.com/user",
            headers={
                "Authorization": f"token {key}",
                "Accept": "application/vnd.github+json",
            },
            timeout=10,
        )
        if r.status_code == 200:
            login = r.json().get("login", "unknown")
            return True, f"GitHub token valid (user: {login})"
        return False, f"GitHub returned {r.status_code}: {r.text[:80]}"
    except Exception as e:
        return False, f"GitHub OAuth error: {e}"


# ---------------------------------------------------------------------------
# Codex  (reuses OpenAI endpoint)
# ---------------------------------------------------------------------------

def validate_codex(key):
    return validate_openai(key)


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

VALIDATORS = {
    "openai":          validate_openai,
    "anthropic":       validate_anthropic,
    "gemini":          validate_gemini,
    "groq":            validate_groq,
    "xai":             validate_xai,
    "replicate":       validate_replicate,
    "perplexity":      validate_perplexity,
    "huggingface":     validate_huggingface,
    "kimi":            validate_kimi,
    "openrouter":      validate_openrouter,
    "amazon_bedrock":  validate_amazon_bedrock,
    "azure_oauth":     validate_azure_oauth,
    "google_oauth":    validate_google_oauth,
    "github_oauth":    validate_github_oauth,
    "codex":           validate_codex,
}


def validate_provider(provider: str, key: str):
    if _is_placeholder(key):
        return False, "Placeholder key ignored"
    func = VALIDATORS.get(provider.lower())
    if not func:
        return False, f"No validator for provider '{provider}'"
    return func(key)
