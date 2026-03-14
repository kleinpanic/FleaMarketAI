#!/usr/bin/env bash
# start-validator.sh — FleaMarketAI v2 Validation Service
# Runs continuously, processing validation queue

set -euo pipefail

VENV_DIR="$(dirname "${BASH_SOURCE[0]}")/.venv"
if [ -d "$VENV_DIR" ]; then
  source "$VENV_DIR/bin/activate"
fi

cd "$(dirname "${BASH_SOURCE[0]}")"

export PYTHONUNBUFFERED=1
export PYTHONDONTWRITEBYTECODE=1

exec python3 -m src.validator
