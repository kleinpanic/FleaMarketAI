# FleaMarket‑AI (AI API Key Hunter & Validator)

A local, self‑hosted service that continuously **searches public internet sources for AI API keys**, validates them against the respective providers, and stores the results in a local SQLite database.

## Goals
- **Discover** API keys for popular AI services (OpenAI, Anthropic, Gemini, Kimi, OpenRouter, Amazon Bedrock, Codex, etc.) in public GitHub repositories, Gists, configuration files, and environment variables.
- **Validate** each key with a lightweight test request (e.g. a cheap completion or model list call) to confirm it works and is still active.
- **Deduplicate** – keep a record of URLs already scanned so we never double‑search.
- **Persist** results (key, provider, validation status, timestamp, source URL) in a local SQLite DB.
- **Run 24/7** on your laptop.  Optionally containerised with **gluetun** for network privacy.
- **Isolated** – no ties to your personal GitHub account; you can host the repo on a separate organization or keep it purely local.

## Quick Start (host mode)
```bash
# Clone the repo (or just use the local copy created by the assistant)
cd ~/codeWS/Python/FleaMarketAI
# Install deps
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# Run the service (will loop forever, sleeping 4 h between runs)
python -m src.main
```

## Docker + Gluetun (optional)
The `docker-compose.yml` starts two containers:
- **gluetun** – provides a VPN‑like tunnel and can expose an HTTP proxy.
- **flea‑market‑ai** – runs the Python service inside the gluetun network.
```bash
cd ~/codeWS/Python/FleaMarketAI
docker compose up -d   # builds and starts the containers
```
The service will log to `logs/` inside the container (mounted as a volume).

## Project Layout
```
FleaMarketAI/
├─ config.yaml                # provider‑specific endpoint config
├─ requirements.txt          # Python dependencies
├─ Dockerfile                # builds the Python app
├─ docker-compose.yml        # gluetun + app services
├─ logs/                     # runtime logs (mounted as volume)
│   └─ processed_urls.txt    # URLs already scanned
├─ db/                       # SQLite DB (mounted as volume)
│   └─ keys.db
└─ src/
    ├─ __init__.py
    ├─ main.py               # orchestrator loop
    ├─ discover.py           # crawls GitHub, Gists, files, env vars
    ├─ validate.py            # provider‑specific validation helpers
    └─ db.py                  # SQLite CRUD utilities
```

## Adding New Providers
1. Add the provider name and a simple test endpoint to `config.yaml`.
2. Implement a function `validate_<provider>(key)` in `src/validate.py` that returns `True/False` and an optional message.
3. Register the function in the `VALIDATORS` mapping at the bottom of `validate.py`.

## Disclaimer
- This tool only **reads public information**; it never accesses private repositories or secrets without explicit permission.
- Use responsibly. Scanning public repos for leaked keys is legal in many jurisdictions, but do not misuse discovered keys.
- The validation calls are cheap (e.g., list models) to avoid costly usage.

---
*Built with love on your self‑hosted DGX Spark‑backed machine*