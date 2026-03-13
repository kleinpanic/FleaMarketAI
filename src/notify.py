"""Discord webhook notifier.

Calls the configured webhook URL with a JSON payload whenever a new
valid key is stored.
"""

import os
import requests

# Load webhook from env or config.yaml (env overrides)
WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

def send_notification(provider, api_key, source_url, validation_msg, line_num=None):
    if not WEBHOOK_URL:
        return False, "No webhook configured"
    # Build a detailed message – include line number if we have it and the full key (user requested un‑masked)
    line_info = f"Line: {line_num}\n" if line_num else ""
    content = (
        f"🔑 **{provider.upper()}** key found and **validated**!\n"
        f"Source: {source_url}\n"
        f"{line_info}"
        f"Result: {validation_msg}\n"
        f"Key: `{api_key}`"
    )
    payload = {"content": content}
    try:
        resp = requests.post(WEBHOOK_URL, json=payload, timeout=5)
        if resp.status_code == 204:
            return True, "Webhook posted"
        return False, f"Webhook error {resp.status_code}: {resp.text}"
    except Exception as e:
        return False, f"Exception posting webhook: {e}"
