"""Discord webhook notifier utilities for FleaMarketAI."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import requests

# Load webhook from env or config.yaml (env overrides)
WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")


def _post(content: str):
    if not WEBHOOK_URL:
        return False, "No webhook configured"
    payload = {"content": content}
    try:
        resp = requests.post(WEBHOOK_URL, json=payload, timeout=5)
        if resp.status_code == 204:
            return True, "Webhook posted"
        return False, f"Webhook error {resp.status_code}: {resp.text}"
    except Exception as e:
        return False, f"Exception posting webhook: {e}"


def _fmt_kv(details: dict[str, Any] | None) -> str:
    if not details:
        return ""
    lines = []
    for k, v in details.items():
        if v is None:
            continue
        lines.append(f"{k}: {v}")
    return "\n".join(lines)


def send_event(stage: str, event: str, details: dict[str, Any] | None = None):
    """Send an internal operational event to Discord.

    Format is intentionally explicit for quick scanning:
    [INTERNAL][<stage>][<event>]
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    header = f"🛰️ **[INTERNAL][{stage.upper()}][{event.upper()}]**"
    body = _fmt_kv(details)
    content = f"{header}\ntimestamp: {ts}\n{body}" if body else f"{header}\ntimestamp: {ts}"
    return _post(content)


def send_notification(
    provider,
    api_key,
    source_url,
    validation_msg,
    line_num=None,
    *,
    origin="discovery",
    event_type="new_valid",
    job_id=None,
    attempt=None,
):
    """Send key-validation notification with clear delineation."""
    if event_type == "new_valid":
        stage = "new_key_validated"
    elif event_type == "revalidated_valid":
        stage = "existing_key_revalidated"
    else:
        stage = str(event_type)

    pipeline = "revalidation_db" if origin == "revalidation" else "discovery_scan"

    line_info = f"source_line: {line_num}\n" if line_num else ""
    job_info = ""
    if job_id is not None:
        job_info += f"job_id: {job_id}\n"
    if attempt is not None:
        job_info += f"attempt: {attempt}\n"

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    content = (
        f"🔑 **[INTERNAL][VALIDATOR][{str(event_type).upper()}]**\n"
        f"timestamp: {ts}\n"
        f"stage: {stage}\n"
        f"pipeline: {pipeline}\n"
        f"provider: {str(provider).upper()}\n"
        f"source_url: {source_url}\n"
        f"{line_info}"
        f"{job_info}"
        f"result: {validation_msg}\n"
        f"key: `{api_key}`"
    )
    return _post(content)

